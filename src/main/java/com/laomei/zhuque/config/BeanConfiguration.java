package com.laomei.zhuque.config;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author luobo
 */
@Configuration
public class BeanConfiguration {

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
