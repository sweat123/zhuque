package com.laomei.zhuque.core;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;

/**
 * @author luobo
 */
@Component
public class ProcessKafkaCollector extends AbstractKafkaCollector {

    @Autowired
    @Qualifier(value = "processKafkaConsumer")
    private KafkaConsumer kafkaConsumer;

    @Override
    public void subscribe(final Collection<String> subscribedTopics) {
        if (!this.subscribedTopics.equals(subscribedTopics)) {
            super.subscribe(subscribedTopics);
            kafkaConsumer.subscribe(subscribedTopics);
        }
    }

    @Override
    public List<KafkaRecord> collect() {
        return null;
    }
}
