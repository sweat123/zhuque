package com.laomei.zhuque.config;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @author luobo
 **/
@Data
@Component
@ConfigurationProperties(prefix = "zhuque.solr")
@NoArgsConstructor
public class SolrProperties {
    private String host;
}
