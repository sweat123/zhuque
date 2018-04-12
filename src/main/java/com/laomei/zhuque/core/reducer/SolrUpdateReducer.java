package com.laomei.zhuque.core.reducer;

import com.laomei.zhuque.core.Context;
import com.laomei.zhuque.core.OffsetCenter;
import com.laomei.zhuque.exception.InitSchemaFailedException;
import com.laomei.zhuque.util.ObjTypeUtil;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * create document with context and update solr collection
 * @author luobo
 **/
public class SolrUpdateReducer implements Reducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SolrUpdateReducer.class);

    private AtomicBoolean isClosed;

    private String solrCollectionName;

    private SolrClient solrClient;

    private Map<String, Class<?>> solrSchemas;

    private Disruptor<MsgEntry> disruptor;

    private RingBuffer<MsgEntry> ringBuffer;

    private EventTranslatorOneArg<MsgEntry, Collection<Context>> TRANSLATOR =
            (event, sequence, arg0) -> {
                event.contexts = arg0;
            };

    public SolrUpdateReducer(@NotNull String solrCollectionName, @NotNull SolrClient solrClient) throws InitSchemaFailedException {
        this.solrCollectionName = solrCollectionName;
        this.solrClient = solrClient;
        isClosed = new AtomicBoolean(false);
        SchemaHelper schemaHelper = new SolrSchemaHepler(solrCollectionName, solrClient);
        try {
            schemaHelper.init();
            solrSchemas = schemaHelper.getSchema();
        } catch (Exception e) {
            throw new InitSchemaFailedException("Get Solr schema from collection: '" + solrCollectionName + "' failed;"
                    + " Exception message: " + e);
        }
        disruptor = new Disruptor<>(new MsgEntryFactory(), 4096, r -> {
            return new Thread(r, "Solr-update-reducer-thread");
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

    private void updateSolr(Collection<Context> contexts) {
        List<SolrInputDocument> documents = new ArrayList<>(contexts.size());
        for (Context context : contexts) {
            if (isClosed.get()) return;
            SolrInputDocument doc = getSolrDocWithContext(context.getUnmodifiableCtx());
            if (doc != null) {
                documents.add(doc);
            }
        }
        if (isClosed.get()) return;
        if (!documents.isEmpty()) {
            updateSolrWithDocs(documents);
            contexts.forEach(context -> OffsetCenter.submit(context.getTopic(), context.getPartition(), context.getOffset()));
        }
    }

    private void updateSolrWithDocs(Collection<SolrInputDocument> documents) {
        try {
            solrClient.add(solrCollectionName, documents);
        } catch (SolrServerException e) {
            LOGGER.error("update solr collection: {} failed; May be there is an error on the server", solrCollectionName, e);
        } catch (IOException e) {
            LOGGER.error("update solr collection: {} failed; May be there is a communication error with the server; Documents: {}",
                    solrCollectionName, documents, e);
        } catch (Exception e) {
            LOGGER.error("unknown exception; solr collection: {};", solrCollectionName, e);
        }
    }

    private SolrInputDocument getSolrDocWithContext(Map<String, Object> context) {
        try {
            SolrInputDocument document = new SolrInputDocument();
            getSolrDocWithContext(context, document);
            return document;
        } catch (Exception e) {
            LOGGER.error("get solr document failed; solr collection: {}, context: {}", solrCollectionName, context, e);
            return null;
        }
    }

    private void getSolrDocWithContext(Map<String, Object> context, SolrInputDocument document) {
        if (isClosed.get()) return;
        for (Map.Entry<String, Object> entry : context.entrySet()) {
            if (isClosed.get()) return;
            String field = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Map) {
                getSolrDocWithContext((Map<String, Object>) value, document);
            } else {
                if (solrSchemas.containsKey(field)) {
                    Class clazz = solrSchemas.get(field);
                    Object valueInSolrType = ObjTypeUtil.convert(value, clazz);
                    document.addField(field, valueInSolrType);
                }
            }
        }
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            disruptor.shutdown();
            solrSchemas.clear();
            solrClient = null;
            disruptor = null;
            ringBuffer = null;
            solrSchemas = null;
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

        SolrUpdateReducer reducer;

        MsgEntryHandler(SolrUpdateReducer reducer) {
            this.reducer = reducer;
        }

        @Override
        public void onEvent(MsgEntry event, long sequence, boolean endOfBatch) throws Exception {
           reducer.updateSolr(event.contexts);
           event.contexts = null;
        }
    }
}
