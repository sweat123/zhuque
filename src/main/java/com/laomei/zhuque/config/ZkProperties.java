package com.laomei.zhuque.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @author luobo
 **/
@Data
@Component
@ConfigurationProperties(value = "zhuque.zookeeper")
public class ZkProperties {

    private String zkUrl;

    private Integer sessionTimeoutMs;

    private Integer connectTimeoutMs;

    private Integer retryTimes;

    private Integer baseSleepTimeMs;
}