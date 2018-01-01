package com.laomei.zhuque.core;

import lombok.Data;

/**
 * Wrap value which polls from Kafka
 * @author luobo
 */
@Data
public class KafkaRecord {

    private String topic;

    private PreProcessRecord preProcessRecord;

    private ProcessRecord processRecord;

    public KafkaRecord(String topic, PreProcessRecord preProcessRecord) {
        this.topic = topic;
        this.preProcessRecord = preProcessRecord;
    }

    public KafkaRecord(String topic, ProcessRecord processRecord) {
        this.topic = topic;
        this.processRecord = processRecord;
    }
    @Data
    public static class PreProcessRecord {
        private Object beforeValue;
        private Object afterValue;

        public PreProcessRecord() {}

        public PreProcessRecord(Object beforeValue, Object afterValue) {
            this.beforeValue = beforeValue;
            this.afterValue = afterValue;
        }
    }

    @Data
    public static class ProcessRecord {
        private Object value;

        public ProcessRecord() {}

        public ProcessRecord(Object value) {
            this.value = value;
        }
    }
}
