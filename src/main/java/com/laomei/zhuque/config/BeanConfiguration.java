package com.laomei.zhuque.config;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author luobo
 */
@Configuration
public class BeanConfiguration {

    /**
     * zookeeper client
     * @param zkProperties zookeeper properties in application.yml
     * @return zkClient
     */
    @Bean
    public CuratorFramework zkClient(ZkProperties zkProperties) {
        return CuratorFrameworkFactory
                .builder()
                .connectString(zkProperties.getZkUrl())
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
    }

    @Bean
    public SolrClient solrClient(SolrProperties properties) {
        return new HttpSolrClient.Builder().withBaseSolrUrl(properties.getHost()).build();
    }
}
