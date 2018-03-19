package com.laomei.zhuque.core.reducer;

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
    Map<String, Class<?>> getSchema() throws Exception;
}
