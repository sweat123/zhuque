package com.laomei.zhuque.core;

import com.laomei.zhuque.core.SyncAssignment.SyncAssignmentPreProcessor;

import java.util.Collection;

/**
 * @author luobo
 */
public interface Router {

    void registry(String task, PreProcessor processor, SyncAssignmentPreProcessor assignment);

    boolean deleteTask(String task, PreProcessor processor);

    void route(KafkaRecord kafkaRecord);

    void route(Collection<KafkaRecord> kafkaRecords);
}
