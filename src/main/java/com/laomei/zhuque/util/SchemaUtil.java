package com.laomei.zhuque.util;

import org.joda.time.DateTime;

/**
 * @author luobo
 */
public class SchemaUtil {


    public static class SolrSchemaType {
        public static Class toJavaType(String clazzImpl) {
            switch (clazzImpl) {
            case "solr.BoolField":
                return Boolean.class;
            case "solr.LongPointField":
                // fall through
            case "solr.TrieLongField":
                return Long.class;
            case "solr.IntPointField":
                // fall through
            case "solr.TrieIntField":
                return Integer.class;
            case "solr.FloatPointField":
                // fall through
            case "solr.TrieFloatField":
                return Float.class;
            case "solr.DoublePointField":
                // fall through
            case "solr.TrieDoubleField":
                return Double.class;
            case "solr.DatePointField":
                // fall through
            case "solr.TrieDateField":
                return DateTime.class;
            default:
                return String.class;
            }
        }
    }
}