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
public class ZkPropperties {

    private String zkUrl;
}