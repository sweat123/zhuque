package com.laomei.zhuque.core;

import lombok.val;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.List;

/**
 * @author luobo
 */
public class PreProcessKafkaCollector extends AbstractKafkaCollector {

    public PreProcessKafkaCollector(KafkaConsumer<GenericRecord, GenericRecord> kafkaConsumer) {
        super(kafkaConsumer);
    }

    @Override
    public List<KafkaRecord> process(ConsumerRecords<GenericRecord, GenericRecord> records)  {
        List<KafkaRecord> kafkaRecords = new ArrayList<>(records.count());
        for (val record : records) {
            if (record.value() == null) {
                continue;
            }
            String topic = record.topic();
            GenericRecord value = record.value();
            Object beforeValue = value.get(FIELD_BEFORE);
            Object afterValue = value.get(FIELD_AFTER);
            kafkaRecords.add(new KafkaRecord(topic, new KafkaRecord.PreProcessRecord(beforeValue, afterValue)));
        }
        return kafkaRecords;
    }
}
