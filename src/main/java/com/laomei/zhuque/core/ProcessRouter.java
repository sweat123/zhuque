package com.laomei.zhuque.core;

import java.util.Collection;

/**
 * @author luobo
 */
public class ProcessRouter implements Router {

    @Override
    public void registry(final String task, final PreProcessor processor,
            final SyncAssignment.SyncAssignmentPreProcessor assignment) {

    }

    @Override
    public boolean deleteTask(final String task, final PreProcessor processor) {
        return false;
    }

    @Override
    public void route(final KafkaRecord kafkaRecord) {

    }

    @Override
    public void route(final Collection<KafkaRecord> kafkaRecords) {

    }
}
