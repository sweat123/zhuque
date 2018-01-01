package com.laomei.zhuque.core;

import com.laomei.zhuque.core.SyncAssignment.SyncAssignmentPreProcessor;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author luobo
 */
public class TopicRouter implements Router {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicRouter.class);

    /**
     * topic => PreProcessor
     */
    private Map<String, Set<PreProcessor>> routeTable;
    /**
     * task => topics
     */
    private Map<String, List<String>> taskWithTopics;
    /**
     * topic => topic subscribed times
     */
    private Map<String, Integer> topicSubscribedTimes;

    public TopicRouter() {
        routeTable = new HashMap<>();
        taskWithTopics = new HashMap<>();
        topicSubscribedTimes = new HashMap<>();
    }

    @Override
    public synchronized void registry(String task, PreProcessor processor, SyncAssignmentPreProcessor assignment) {
        if (assignment.getTopicConfigs() != null && !assignment.getTopicConfigs().isEmpty()) {
            List<String> topics = taskWithTopics.computeIfAbsent(task, k -> new ArrayList<>());
            for (val topicConfig : assignment.getTopicConfigs()) {
                String topic = topicConfig.getTopic();
                topics.add(topic);
                Set<PreProcessor> preProcessors = routeTable.computeIfAbsent(topic, k -> new HashSet<>());
                preProcessors.add(processor);
                int subscribedNum = 0;
                if (topicSubscribedTimes.get(topic) != null) {
                    subscribedNum = topicSubscribedTimes.get(topic);
                }
                topicSubscribedTimes.put(topic, ++ subscribedNum);
            }
        }
    }

    @Override
    public synchronized boolean deleteTask(String task, PreProcessor processor) {
        List<String> topics = taskWithTopics.remove(task);
        if (topics == null) {
            LOGGER.error("task {} not exist.", task);
            return false;
        }
        topics.forEach(topic -> {
            routeTable.get(topic).remove(processor);
            Integer num = topicSubscribedTimes.get(topic);
            if (num != null && num == 1) {
                topicSubscribedTimes.remove(topic);
            } else if (num != null) {
                topicSubscribedTimes.put(topic, -- num);
            }
        });
        return true;
    }

    @Override
    public synchronized void route(KafkaRecord kafkaRecord) {
        String topic = kafkaRecord.getTopic();
        Set<PreProcessor> processors = routeTable.get(topic);
        //TODO: process may be blocked; If it's blocked, it may affect following processing;
        processors.forEach(processor -> processor.process(kafkaRecord));
    }

    @Override
    public synchronized void route(final Collection<KafkaRecord> kafkaRecords) {
        for (val kafkaRecord : kafkaRecords) {
            route(kafkaRecord);
        }
    }
}
