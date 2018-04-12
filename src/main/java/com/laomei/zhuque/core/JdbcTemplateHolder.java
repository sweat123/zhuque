package com.laomei.zhuque.core;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.util.HashMap;

/**
 * @author luobo
 */
public class JdbcTemplateHolder {

    private static HashMap<String, JdbcTemplate> jdbcTemplateMap = new HashMap<>();

    public synchronized static void registry(String url, String username, String password) {
        if (jdbcTemplateMap.get(url) == null) {
            DriverManagerDataSource dataSource = new DriverManagerDataSource();
            dataSource.setDriverClassName("com.mysql.jdbc.Driver");
            dataSource.setUrl(url);
            dataSource.setUsername(username);
            dataSource.setPassword(password);
            jdbcTemplateMap.put(url, new JdbcTemplate(dataSource));
        }
    }

    public synchronized static JdbcTemplate getJdbcTemplate(String key) {
        return jdbcTemplateMap.get(key);
    }

    public synchronized static void clear() {
        jdbcTemplateMap.clear();
    }
}
