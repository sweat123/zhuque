package com.laomei.zhuque.core;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;

/**
 * @author luobo
 **/
@Component
public class PreProcessor {

    @Autowired
    @Qualifier(value = "preProcessKafkaCollector")
    private Collector kafkaCollector;

    private Set<String> subscription;

    public PreProcessor() {
        subscription = new HashSet<>();
    }

    public void addTopic(String topic) {
        if (!subscription.contains(topic)) {
            subscription.add(topic);
            kafkaCollector.subscribe(subscription);
        }
    }

    public boolean deleteTopic(String topic) {
        boolean deleted = subscription.remove(topic);
        if (deleted) {
            kafkaCollector.subscribe(subscription);
        }
        return deleted;
    }
}
