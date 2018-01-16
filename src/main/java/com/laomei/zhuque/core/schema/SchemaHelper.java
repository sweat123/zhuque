package com.laomei.zhuque.core.schema;

import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;
import java.util.Map;

/**
 * @author luobo
 */
public interface SchemaHelper {

    /**
     * init schema helper; such as connect to solr or mysql;
     */
    void init();

    /**
     * get schema from solr, mysql and so on;
     * @return fields and the clazz of the fields
     */
    Map<String, Class<?>> getSchema() throws IOException, SolrServerException;
}
