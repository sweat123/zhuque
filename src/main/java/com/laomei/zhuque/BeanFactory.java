package com.laomei.zhuque;

import org.apache.avro.generic.GenericRecord;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Description;

import java.util.Map;

/**
 * @author luobo
 */
@Configuration
public class BeanFactory {

    @Bean
    public KafkaProducer<?, ?> kafkaProducer(KafkaProperties props) {
        Map<String, Object> config = props.buildKafkaProducerProps();
        return new KafkaProducer<>(config);
    }

    /**
     * kafka consumer for preProcess part
     * @param props kafka properties in application.yml
     * @return KafkaConsumer
     */
    @Bean(name = "preProcessKafkaConsumer")
    @Description("kafka consumer for preProcess part")
    public KafkaConsumer<GenericRecord, GenericRecord> preProcessorKafkaConsumer(KafkaProperties props) {
        Map<String, Object> config = props.buildKafkaConsumerProps();
        return new KafkaConsumer<>(config);
    }

    /**
     * kafka consumer for process part
     * @param props kafka properties in application.yml
     * @return KafkaConsumer
     */
    @Bean(name = "processKafkaConsumer")
    @Description("kafka consumer for process part")
    public KafkaConsumer<?, ?> processKafkaConsumer(KafkaProperties props) {
        Map<String, Object> config = props.buildKafkaConsumerProps();
        return new KafkaConsumer<>(config);
    }

    /**
     * zookeeper client
     * @param zkPropperties zookeeper properties in application.yml
     * @return zkClient
     */
    @Bean
    public CuratorFramework zkClient(ZkPropperties zkPropperties) {
        return CuratorFrameworkFactory
                .builder()
                .connectString(zkPropperties.getZkUrl())
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
    }
}
