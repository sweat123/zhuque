package com.laomei.zhuque.core;

import lombok.Data;

/**
 * Wrap value which polls from Kafka
 * @author luobo
 */
@Data
public class KafkaRecord {

    private String topic;
    private Object beforeValue;
    private Object afterValue;

    public KafkaRecord(String topic, Object beforeValue, Object afterValue) {
        this.topic = topic;
        this.beforeValue = beforeValue;
        this.afterValue = afterValue;
    }
}
