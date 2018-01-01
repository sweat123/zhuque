package com.laomei.zhuque.core;

import lombok.val;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author luobo
 */
@Component
public class PreProcessKafkaCollector extends AbstractKafkaCollector {

    @Autowired
    @Qualifier(value = "preProcessKafkaConsumer")
    private KafkaConsumer<GenericRecord, GenericRecord> kafkaConsumer;

    @Override
    public void subscribe(final Collection<String> subscribedTopics) {
        if (!this.subscribedTopics.equals(subscribedTopics)) {
            super.subscribe(subscribedTopics);
            kafkaConsumer.subscribe(subscribedTopics);
        }
    }

    @Override
    public List<KafkaRecord> collect() {
        ConsumerRecords<GenericRecord, GenericRecord> records = kafkaConsumer.poll(POLL_TIME_MS);
        if (records.count() == 0) {
            return Collections.emptyList();
        }
        return preProcess(records);
    }

    private List<KafkaRecord> preProcess(ConsumerRecords<GenericRecord, GenericRecord> records)  {
        List<KafkaRecord> kafkaRecords = new ArrayList<>(records.count());
        for (val record : records) {
            if (record.value() == null) {
                continue;
            }
            String topic = record.topic();
            GenericRecord value = record.value();
            Object beforeValue = value.get(FIELD_BEFORE);
            Object afterValue = value.get(FIELD_AFTER);
            kafkaRecords.add(new KafkaRecord(topic, beforeValue, afterValue));
        }
        return kafkaRecords;
    }
}
