package com.laomei.zhuque.core;

import java.util.List;
import java.util.Map;

/**
 * @author luobo
 */
public interface Processor  {

    String PROCESS_KAFKA_RECORD_BEFORE_VALUE = "pre_process_kafka_record_before_value";
    String PROCESS_KAFKA_RECORD_AFTER_VALUE = "pre_process_kafka_record_after_value";

    Context process(KafkaRecord record);

    List<Context> process(List<KafkaRecord> records);

    void close();
}
