package com.laomei.zhuque.config;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author luobo
 */
@Data
@Component
@ConfigurationProperties(prefix = "zhuque.kafka")
@NoArgsConstructor
public class KafkaProperties {

    private List<String> kafkaBootstrap;

    private KafkaConsumerProps consumer;

    private Map<String, Object> buildCommonProps() {
        Map<String, Object> props = new HashMap<>();
        if (kafkaBootstrap == null) {
            throw new NullPointerException("kafkaBootstrap is null;");
        }
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
        return props;
    }

    public Map<String, Object> buildKafkaConsumerProps() {
        Map<String, Object> props = buildCommonProps();
        props.putAll(consumer.buildProps());
        return props;
    }

    @Data
    @NoArgsConstructor
    public static class KafkaConsumerProps {
        static final String SCHEMA_REGISTRY_URL = "schema.registry.url";

        private Class<?> keySerializer = io.confluent.kafka.serializers.KafkaAvroDeserializer.class;

        private Class<?> valueSerializer = io.confluent.kafka.serializers.KafkaAvroDeserializer.class;;

        private Integer maxPollIntervalMs;

        private Integer heartbeatIntervalMs;

        private Integer sessionTimeoutMs;

        private Integer maxPollRecords;

        private Boolean enableAutoCommit;

        private String autoOffsetReset;

        private String schemaRegistryUrl;

        private Map<String, Object> otherProps;

        Map<String, Object> buildProps() {
            Map<String, Object> props = new HashMap<>();
            if (keySerializer != null) {
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keySerializer);
            }
            if (valueSerializer != null) {
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueSerializer);
            }
            if (maxPollIntervalMs != null) {
                props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
            }
            if (heartbeatIntervalMs != null) {
                props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);
            }
            if (sessionTimeoutMs != null) {
                props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
            }
            if (maxPollRecords != null) {
                props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
            }
            if (enableAutoCommit != null) {
                props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
            }
            if (autoOffsetReset != null) {
                props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
            }
            if (schemaRegistryUrl != null) {
                props.put(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
            }
            if (otherProps != null) {
                for (Map.Entry<String, Object> entry : otherProps.entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    props.put(key, value);
                }
            }
            return props;
        }
    }
}
