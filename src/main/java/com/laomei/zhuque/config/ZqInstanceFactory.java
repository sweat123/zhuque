package com.laomei.zhuque.config;

import org.apache.avro.generic.GenericRecord;
import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.HashMap;
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

    private Map<String, JdbcTemplate> jdbcTemplateMap;

    public ZqInstanceFactory() {
        jdbcTemplateMap = new HashMap<>();
    }

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

    /**
     * get CuratorFramework
     * @return CuratorFramework
     */
    public CuratorFramework zkClient() {
        return zkCli;
    }

    /**
     * get JdbcTemplate with table address, user and password
     * @param address table address
     * @param user username
     * @param password password
     * @return JdbcTemplate
     */
    public JdbcTemplate jdbcTemplate(String address, String user, String password) {
        return jdbcTemplateMap.computeIfAbsent(address, ignore -> {
            DataSource dataSource = new DriverManagerDataSource(address, user, password);
            return new JdbcTemplate(dataSource);
        });
    }
}
