package com.laomei.zhuque;

import com.laomei.zhuque.config.KafkaProperties;
import org.apache.avro.generic.GenericRecord;
import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * @author luobo
 */
@Component
public class ZqInstanceFactory {

    @Autowired
    private CuratorFramework zkCli;

    @Autowired
    private KafkaProducer<?, ?> kafkaProducer;

    /**
     * kafka consumer for preProcess part
     * @param props kafka properties in application.yml
     * @return KafkaConsumer
     */
    public KafkaConsumer<GenericRecord, GenericRecord> preProcessorKafkaConsumer(KafkaProperties props) {
        Map<String, Object> config = props.buildKafkaConsumerProps();
        return new KafkaConsumer<>(config);
    }

    /**
     * kafka consumer for process part
     * @param props kafka properties in application.yml
     * @return KafkaConsumer
     */
    public KafkaConsumer<?, ?> processKafkaConsumer(KafkaProperties props) {
        Map<String, Object> config = props.buildKafkaConsumerProps();
        return new KafkaConsumer<>(config);
    }

    public KafkaProducer<?, ?> kafkaProducer() {
        return kafkaProducer;
    }

    public CuratorFramework zkClient() {
        return zkCli;
    }
}
