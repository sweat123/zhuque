package com.laomei.zhuque.core.reducer;

import com.laomei.zhuque.core.reducer.schema.SchemaHelper;
import com.laomei.zhuque.core.reducer.schema.SolrSchemaHepler;
import com.laomei.zhuque.exception.InitSchemaFailedException;
import com.laomei.zhuque.util.ObjTypeUtil;
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

/**
 * create document with context and update solr collection
 * @author luobo
 **/
public class SolrUpdateReducer implements Reducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SolrUpdateReducer.class);

    private String solrCollectionName;

    private SolrClient solrClient;

    private Map<String, Class<?>> solrSchemas;

    public SolrUpdateReducer(@NotNull String solrCollectionName, @NotNull SolrClient solrClient) throws InitSchemaFailedException {
        this.solrCollectionName = solrCollectionName;
        this.solrClient = solrClient;
        SchemaHelper schemaHelper = new SolrSchemaHepler(solrCollectionName, solrClient);
        try {
            schemaHelper.init();
            schemaHelper.getSchema();
            solrSchemas = schemaHelper.getSchema();
        } catch (Exception e) {
            throw new InitSchemaFailedException("Get Solr schema from collection: " + solrCollectionName + " failed;"
                    + " Exception message: " + e);
        }
    }

    @Override
    public void reduce(Collection<Map<String, Object>> contexts) {
        List<SolrInputDocument> documents = new ArrayList<>();
        for (Map<String, Object> context : contexts) {
            SolrInputDocument doc = getSolrDocWithContext(context);
            if (doc != null) {
                documents.add(doc);
            }
        }
        if (!documents.isEmpty()) {
            updateSolrWithDocs(documents);
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
        for (Map.Entry<String, Object> entry : context.entrySet()) {
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
        solrSchemas.clear();
        solrClient = null;
    }
}
