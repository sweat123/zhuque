package com.laomei.zhuque.core;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;

/**
 * @author luobo
 */
public class ProcessKafkaCollector extends AbstractKafkaCollector {

    public ProcessKafkaCollector(KafkaConsumer<GenericRecord, GenericRecord> kafkaConsumer) {
        super(kafkaConsumer);
    }

    @Override
    public List<KafkaRecord> process(final ConsumerRecords<GenericRecord, GenericRecord> records) {
        //translate records which from ZhuQue topic to kafka records;
        return null;
    }
}
