package com.laomei.zhuque.core;

import java.util.Collection;
import java.util.List;

/**
 * Working for collecting data from kafka and send data to transformers
 *
 * @author luobo
 */
public interface Collector {

    int POLL_TIME_MS = 100;

    /**
     * data format which debezium get from mysql
     */
    String FIELD_BEFORE = "before";
    String FIELD_AFTER = "after";

    /**
     * assign topics to subscribe
     * @param subscribedTopics topics
     */
    void subscribe(Collection<String> subscribedTopics);

    /**
     * get subscribed topics
     * @return topics
     */
    Collection<String> subscribedTopics();

    /**
     * collect data from kafka
     * @return
     */
    List<KafkaRecord> collect();

    /**
     * release resources
     */
    void close();
}
