package com.laomei.zhuque.core;

import lombok.Data;
import lombok.val;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author luobo
 */
@Data
public class Metadata {

    /**
     * topic which is subscribed by preProcess KafkaConsumer
     */
    private Set<String> preProcessSubscription;

    /**
     * topic which is subscribed by process KafkaConsumer
     */
    private Set<String> processSubscription;

    /**
     * all sync assignments
     */
    private Map<String, SyncAssignment> assignments;

    private boolean needUpdate;

    public Metadata() {
        preProcessSubscription = new HashSet<>();
        processSubscription = new HashSet<>();
    }

    public synchronized boolean contain(String taskName) {
        return assignments.containsKey(taskName);
    }

    public synchronized void addAssignment(String taskName, SyncAssignment assignment) {
        assignments.put(taskName, assignment);
        SyncAssignment.SyncAssignmentPreProcessor preProcessor = assignment.getPreProcessor();
        if (preProcessor != null && preProcessor.getTopicConfigs() != null && !preProcessor.getTopicConfigs().isEmpty()) {
            boolean addNewTopic = false;
            for (val topicConfig : preProcessor.getTopicConfigs()) {
                if (!preProcessSubscription.contains(topicConfig.getTopic())) {
                    preProcessSubscription.add(topicConfig.getTopic());
                    addNewTopic = true;
                }
            }
            if (addNewTopic) {
                requestUpdate();
            }
        }
        SyncAssignment.SyncAssignmentProcessor processor = assignment.getProcessor();
        if (processor != null && processor.getKafkaTopic() != null) {
            processSubscription.add(processor.getKafkaTopic());
            requestUpdate();
        }
    }

    public synchronized void deleteAssignment(String taskName) {
        if (assignments.containsKey(taskName)) {
            SyncAssignment assignment = assignments.remove(taskName);
            if (assignment != null) {
                //remove topic from preProcessSubscription
                SyncAssignment.SyncAssignmentPreProcessor preProcessor = assignment.getPreProcessor();
                if (preProcessor != null && preProcessor.getTopicConfigs() != null && !preProcessor.getTopicConfigs().isEmpty()) {
                    for (val topicConfig: preProcessor.getTopicConfigs()) {
                        preProcessSubscription.remove(topicConfig.getTopic());
                    }
                    requestUpdate();
                }
                //remove topic from processSubscription
                SyncAssignment.SyncAssignmentProcessor processor = assignment.getProcessor();
                if (processor != null && processor.getKafkaTopic() != null) {
                    processSubscription.remove(processor.getKafkaTopic());
                    requestUpdate();
                }
            }
        }
    }

    public synchronized Set<String> preProcessSubscription() {
        return preProcessSubscription;
    }

    public synchronized Set<String> processSubscription() {
        return processSubscription;
    }

    public synchronized boolean updateRequested() {
        return needUpdate;
    }

    private synchronized void requestUpdate() {
        needUpdate = true;
    }
}
