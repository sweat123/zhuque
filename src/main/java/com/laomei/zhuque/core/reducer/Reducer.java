package com.laomei.zhuque.core.reducer;

import com.laomei.zhuque.core.Context;

import java.util.Collection;

/**
 * @author luobo
 */
public interface Reducer {

    String SOLR_UPDATE_REDUCER = "SolrUpdateReducer";
    String SOLR_DELETE_REDUCER = "SolrDeleteReducer";
    String MYSQL_REDUCER = "MysqlReducer";

    /**
     * update solr, mysql or others with contexts;
     * @param contexts the value for make document
     */
    void reduce(Collection<Context> contexts);

    /**
     * close and release resources;
     */
    void close();
}
