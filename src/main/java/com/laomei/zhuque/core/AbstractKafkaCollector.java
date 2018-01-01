package com.laomei.zhuque.core;

import java.util.Collection;
import java.util.Collections;

/**
 * @author luobo
 */
public abstract class AbstractKafkaCollector implements Collector {

    Collection<String> subscribedTopics;

    public AbstractKafkaCollector() {
        this.subscribedTopics = Collections.emptySet();
    }

    @Override
    public void subscribe(final Collection<String> subscribedTopics) {
        this.subscribedTopics = subscribedTopics;
    }

    @Override
    public Collection<String> subscribedTopics() {
        return subscribedTopics;
    }


}
