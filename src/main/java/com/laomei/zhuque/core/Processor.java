package com.laomei.zhuque.core;

import java.util.List;

/**
 * @author luobo
 */
public interface Processor extends AutoCloseable {
    Object process(KafkaRecord record);

    Object process(List<KafkaRecord> records);
}
