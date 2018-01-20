package com.laomei.zhuque.core.reducer;

import java.util.Collection;
import java.util.Map;

/**
 * @author luobo
 */
public interface Reducer {

    /**
     * update solr, mysql or others with contexts;
     * @param contexts the value for make document
     */
    void reduce(Collection<Map<String, Object>> contexts);

    /**
     * close and release resources;
     */
    void close();
}
