package com.laomei.zhuque.config;

import com.laomei.zhuque.core.SyncAssignment;
import org.apache.avro.generic.GenericRecord;
import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.solr.client.solrj.SolrClient;
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
    private KafkaProperties props;

    @Autowired
    private SolrClient solrClient;

    private Map<String, JdbcTemplate> jdbcTemplateMap;

    public ZqInstanceFactory() {
        jdbcTemplateMap = new HashMap<>();
    }

    /**
     * kafka consumer for collecting record
     * @return KafkaConsumer
     */
    public KafkaConsumer<GenericRecord, GenericRecord> kafkaConsumer(String groupId, String offsetPolicy) {
        if (props != null) {
            Map<String, Object> config = props.buildKafkaConsumerProps();
            config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetPolicy);
            return new KafkaConsumer<>(config);
        } else {
            throw new NullPointerException("kafka consumer properties can't be null;");
        }
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
     * @param mysqlConfig MysqlConfig
     * @return JdbcTemplate
     */
    public JdbcTemplate jdbcTemplate(SyncAssignment.MysqlConfig mysqlConfig) {
        return jdbcTemplateMap.computeIfAbsent(mysqlConfig.getAddress(), ignore -> {
            DataSource dataSource = new DriverManagerDataSource(mysqlConfig.getAddress(),
                    mysqlConfig.getUsername(), mysqlConfig.getPassword());
            return new JdbcTemplate(dataSource);
        });
    }

    public SolrClient solrClient() {
        return solrClient;
    }
}
