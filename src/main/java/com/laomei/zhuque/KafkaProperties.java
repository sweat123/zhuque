package com.laomei.zhuque;

import lombok.Data;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
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
public class KafkaProperties {

    private List<String> kafkaBootstrap;

    private Map<String, Object> buildCommonProps() {
        Map<String, Object> props = new HashMap<>();
        if (kafkaBootstrap == null) {
            throw new NullPointerException("kafkaBootstrap is null;");
        }
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
        return props;
    }

    Map<String, Object> buildKafkaProducerProps() {
        KafkaProducerProps kafkaProducerProps = new KafkaProducerProps();
        Map<String, Object> props = buildCommonProps();
        props.putAll(kafkaProducerProps.buildProps());
        return props;
    }

    Map<String, Object> buildKafkaConsumerProps() {
        KafkaConsumerProps kafkaConsumerProps = new KafkaConsumerProps();
        Map<String, Object> props = buildCommonProps();
        props.putAll(kafkaConsumerProps.buildProps());
        return props;
    }

    @Data
    static class KafkaProducerProps {

        private Class<?> keySerializer;

        private Class<?> valueSerializer;

        Map<String, Object> buildProps() {
            Map<String, Object> props = new HashMap<>();
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
            return props;
        }
    }

    @Data
    static class KafkaConsumerProps {

        private Class<?> keySerializer;

        private Class<?> valueSerializer;

        private Integer maxPollIntervalMs;

        private Integer heartbeatIntervalMs;

        private Integer sessionTimeoutMs;

        private Integer maxPollRecords;

        private Boolean enableAutoCommit;

        private String autoOffsetReset;

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
            return props;
        }
    }
}
