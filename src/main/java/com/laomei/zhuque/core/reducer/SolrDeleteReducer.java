package com.laomei.zhuque.core.reducer;

import com.laomei.zhuque.constants.ZhuQueConstants;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * delete solr index with ids;
 * The schema of solr collection must has a filed named id;
 * @author luobo
 **/
public class SolrDeleteReducer implements Reducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SolrDeleteReducer.class);

    private String solrCollectionName;

    private SolrClient solrClient;

    private Disruptor<MsgEntry> disruptor;

    private RingBuffer<MsgEntry> ringBuffer;

    private EventTranslatorOneArg<MsgEntry, Collection<Map<String, Object>>> TRANSLATOR =
            (event, sequence, arg0) -> {
                event.contexts = arg0;
            };

    public SolrDeleteReducer(@NotNull String solrCollectionName, @NotNull SolrClient solrClient) {
        this.solrCollectionName = solrCollectionName;
        this.solrClient = solrClient;
        disruptor = new Disruptor<>(new MsgEntryFactory(), 4096, r -> {
            return new Thread(r, "Solr-delete-reducer-thread");
        });
        disruptor.handleEventsWith(new MsgEntryHandler(this));
        ringBuffer = disruptor.getRingBuffer();
        disruptor.start();
    }

    @Override
    public void reduce(Collection<Map<String, Object>> contexts) {
        ringBuffer.publishEvent(TRANSLATOR, contexts);
    }

    @Override
    public void close() {
        disruptor.shutdown();
        solrClient = null;
    }

    private void deleteSolrIndex(Collection<Map<String, Object>> contexts) {
        List<String> deleteDocIds = new ArrayList<>(contexts.size());
        for (Map<String, Object> context : contexts) {
            Object docId = context.get(ZhuQueConstants.SOLR_CONTEXT_ID);
            if (docId != null) {
                deleteDocIds.add(String.valueOf(docId));
            }
        }
        if (!deleteDocIds.isEmpty()) {
            deleteSolrWithDocIds(deleteDocIds);
        }
    }

    private void deleteSolrWithDocIds(List<String> docIds) {
        try {
            solrClient.deleteById(solrCollectionName, docIds);
        } catch (SolrServerException e) {
            LOGGER.error("solr collection {} delete index failed; May be there is an error on the server", solrCollectionName, e);
        } catch (IOException e) {
            LOGGER.error("solr collection: {} delete index failed; May be there is a low-level I/O error.", solrCollectionName, e);
        } catch (Exception e) {
            LOGGER.error("unknown exception; solr collection: {};", solrCollectionName, e);
        }
    }

    // belows are contexts wrapper for disruptor

    static class MsgEntry {
        Collection<Map<String, Object>> contexts;

        MsgEntry(Collection<Map<String, Object>> contexts) {
            this.contexts = contexts;
        }
    }

    static class MsgEntryFactory implements EventFactory<MsgEntry> {
        @Override
        public MsgEntry newInstance() {
            return new MsgEntry(null);
        }
    }

    static class MsgEntryHandler implements EventHandler<MsgEntry> {

        SolrDeleteReducer reducer;

        MsgEntryHandler(SolrDeleteReducer reducer) {
            this.reducer = reducer;
        }

        @Override
        public void onEvent(MsgEntry event, long sequence, boolean endOfBatch) throws Exception {
            reducer.deleteSolrIndex(event.contexts);
            event.contexts = null;
        }
    }
}
