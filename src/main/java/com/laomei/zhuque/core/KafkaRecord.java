package com.laomei.zhuque.core;

import lombok.Data;

/**
 * Wrap value which polls from Kafka
 * @author luobo
 */
@Data
public class KafkaRecord {

    private String topic;

    private Object before;

    private Object after;

    public KafkaRecord(String topic, Object before, Object after) {
        this.topic = topic;
        this.before = before;
        this.after = after;
    }
}
