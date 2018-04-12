package com.laomei.zhuque.core;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * @author luobo
 **/
public class OffsetCenter {
    private static final long INIT = -1;

    private static final Map<String, Map<Integer, Long>> OFFSETS = new HashMap<>();

    public static synchronized void submit(String topic, int partition, long offset) {
        Map<Integer, Long> highestOffsetMap = OFFSETS.get(topic);
        if (!highestOffsetMap.containsKey(partition)) {
            //如果partition不在highestOffsetMap内，说明这个partition
            //已经从此进程的KafkaConsumer里移除掉，我们不应该将这个offset添加
            //新加入的KafkaConsumer实例会消费处理该offset
            return;
        }
        Long o = highestOffsetMap.get(partition);
        if (o == INIT || offset > o) {
            highestOffsetMap.put(partition, offset);
        }
    }

    public static synchronized Map<TopicPartition, OffsetAndMetadata> offset() {
        if (OFFSETS.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
        for (Map.Entry<String, Map<Integer, Long>> entry : OFFSETS.entrySet()) {
            for (Map.Entry<Integer, Long> offset : entry.getValue().entrySet()) {
                if (offset.getValue() != INIT) {
                    //partition的offset为INIT不能被commit
                    commits.put(new TopicPartition(entry.getKey(), offset.getKey()),
                            new OffsetAndMetadata(offset.getValue()));
                }
            }
        }
        return commits;
    }

    /**
     * 1. 第一次加入，需要创建Topic对应的Map.
     * 2. 中途加入，移除未订阅的partition，添加新的partition信息, 添加新的topic信息.
     *
     * KafkaConsumer所在的线程维护管理Topic和partition信息。
     * Reducer所在线程维护offset信息。
     * @param partitions TopicPartitions
     */
    public static synchronized void updatePartitionInfo(Collection<TopicPartition> partitions) {
        //如果我们开启了多个进程，并且使用同一个group id，也就是正在运行的
        //consumer它的分区会被re assign给其它consumer实例，
        //这时候我们要更新当前进程内，OffsetCenter维护的Topic, partition, offset 信息；
        //需要删除当前进程里consumer没有订阅的partition数据.
        final Map<String, Set<Integer>> availablePartition = new HashMap<>();
        //availablePartition key为Topic，value为partition集合
        partitions.forEach(partition -> {
            String topic = partition.topic();
            Set<Integer> p = availablePartition.computeIfAbsent(topic, k -> new HashSet<>());
            p.add(partition.partition());
        });
        //移除不需要订阅的partition
        availablePartition.forEach((topic, subscribedPartitions) -> {
            if (OFFSETS.get(topic) != null) {
                OFFSETS.get(topic).entrySet().removeIf(entry -> !subscribedPartitions.contains(entry.getKey()));
            }
        });
        //增加新的Topic信息, 同时增加新的partition信息
        partitions.forEach(topicPartition -> {
            String topic = topicPartition.topic();
            int partition = topicPartition.partition();
            if (!OFFSETS.containsKey(topic)) {
                OFFSETS.put(topic, new HashMap<>());
            }
            Map<Integer, Long> offsets = OFFSETS.get(topic);
            if (!offsets.containsKey(partition)) {
                //订阅了该分区，但是还不知道此分区的offset，
                //就把他标记为INIT.
                offsets.put(partition, INIT);
            }
        });
    }
}
