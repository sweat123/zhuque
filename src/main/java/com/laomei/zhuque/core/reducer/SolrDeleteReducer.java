package com.laomei.zhuque.core.reducer;

import com.laomei.zhuque.constants.ZhuQueConstants;
import com.laomei.zhuque.core.Context;
import com.laomei.zhuque.core.OffsetCenter;
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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * delete solr index with ids;
 * The schema of solr collection must has a filed named id;
 * @author luobo
 **/
public class SolrDeleteReducer implements Reducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SolrDeleteReducer.class);

    private AtomicBoolean isClosed;

    private String solrCollectionName;

    private SolrClient solrClient;

    private Disruptor<MsgEntry> disruptor;

    private RingBuffer<MsgEntry> ringBuffer;

    private EventTranslatorOneArg<MsgEntry, Collection<Context>> TRANSLATOR =
            (event, sequence, arg0) -> {
                event.contexts = arg0;
            };

    public SolrDeleteReducer(@NotNull String solrCollectionName, @NotNull SolrClient solrClient) {
        this.solrCollectionName = solrCollectionName;
        this.solrClient = solrClient;
        isClosed = new AtomicBoolean(false);
        disruptor = new Disruptor<>(new MsgEntryFactory(), 4096, r -> {
            return new Thread(r, "Solr-delete-reducer-thread");
        });
        disruptor.handleEventsWith(new MsgEntryHandler(this));
        ringBuffer = disruptor.getRingBuffer();
        disruptor.start();
    }

    @Override
    public void reduce(Collection<Context> contexts) {
        if (isClosed.get()) return;
        ringBuffer.publishEvent(TRANSLATOR, contexts);
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            disruptor.shutdown();
            solrClient = null;
            disruptor = null;
            ringBuffer = null;
        }
    }

    private void deleteSolrIndex(Collection<Context> contexts) {
        if (isClosed.get()) return;
        List<String> deleteDocIds = new ArrayList<>(contexts.size());
        for (Context context : contexts) {
            if (isClosed.get()) return;
            Object docId = context.get(ZhuQueConstants.SOLR_CONTEXT_ID);
            if (docId != null) {
                deleteDocIds.add(String.valueOf(docId));
            }
        }
        if (isClosed.get()) return;
        if (!deleteDocIds.isEmpty()) {
            deleteSolrWithDocIds(deleteDocIds);
            contexts.forEach(context -> OffsetCenter.submit(context.getTopic(), context.getPartition(), context.getOffset()));

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
        Collection<Context> contexts;

        MsgEntry(Collection<Context> contexts) {
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
