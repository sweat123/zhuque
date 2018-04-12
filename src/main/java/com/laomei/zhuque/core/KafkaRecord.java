package com.laomei.zhuque.core;

import lombok.Data;

/**
 * Wrap value which polls from Kafka
 * @author luobo
 */
@Data
public class KafkaRecord {

    private String topic;

    private int partition;

    private long offset;

    private Object before;

    private Object after;

    public KafkaRecord(String topic, int partition, long offset, Object before, Object after) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.before = before;
        this.after = after;
    }
}
