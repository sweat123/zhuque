package com.laomei.zhuque.core;

import java.util.List;
import java.util.Map;

/**
 * @author luobo
 */
public interface Processor extends AutoCloseable {

    String PREPROCESS_KAFKA_RECORD_BEFORE_VALUE = "pre_process_kafka_record_before_value";
    String PREPROCESS_KAFKA_RECORD_AFTER_VALUE = "pre_process_kafka_record_after_value";

    Map<String, Object> process(KafkaRecord record);

    List<Map<String, Object>> process(List<KafkaRecord> records);
}
