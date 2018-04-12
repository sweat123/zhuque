package com.laomei.zhuque.core;

import lombok.val;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author luobo
 */
public class KafkaCollector implements Collector {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaCollector.class);

    private Collection<String> subscribedTopics;

    private KafkaConsumer<GenericRecord, GenericRecord> kafkaConsumer;

    public KafkaCollector(KafkaConsumer<GenericRecord, GenericRecord> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
        this.subscribedTopics = Collections.emptySet();
    }

    @Override
    public Collection<String> subscribedTopics() {
        return subscribedTopics;
    }

    @Override
    public void subscribe(final Collection<String> subscribedTopics) {
        if (!this.subscribedTopics.equals(subscribedTopics)) {
            this.subscribedTopics = subscribedTopics;
            kafkaConsumer.subscribe(subscribedTopics);
            kafkaConsumer.subscribe(subscribedTopics, new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
                    LOGGER.info("kafka partitions revoked: {}; commit offset;", partitions);
                    kafkaConsumer.commitSync(OffsetCenter.offset());
                }

                @Override
                public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
                    LOGGER.info("kafka partitions assigned: {}; 更新OffsetCenter;", partitions);
                    OffsetCenter.updatePartitionInfo(partitions);
                }
            });
        }
    }

    @Override
    public List<KafkaRecord> collect() {
        ConsumerRecords<GenericRecord, GenericRecord> records = kafkaConsumer.poll(POLL_TIME_MS);
        if (records.count() == 0) {
            return Collections.emptyList();
        }
        return process(records);
    }

    @Override
    public void close() {
        if (kafkaConsumer != null) {
            kafkaConsumer.close();
            subscribedTopics.clear();
        }
    }

    /**
     * process ConsumerRecords which from kafka, and translate them to KafkaRecord;
     * @param records ConsumerRecords
     * @return kafka records
     */
    public List<KafkaRecord> process(ConsumerRecords<GenericRecord, GenericRecord> records)  {
        List<KafkaRecord> kafkaRecords = new ArrayList<>(records.count());
        for (val record : records) {
            if (record.value() == null) {
                continue;
            }
            String topic = record.topic();
            int partition = record.partition();
            long offset = record.offset();
            GenericRecord value = record.value();
            Object beforeValue = value.get(FIELD_BEFORE);
            Object afterValue = value.get(FIELD_AFTER);
            kafkaRecords.add(new KafkaRecord(topic, partition, offset, beforeValue, afterValue));
        }
        return kafkaRecords;
    }
}
