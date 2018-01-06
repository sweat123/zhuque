package com.laomei.zhuque.core.transformation;

import com.google.common.base.Preconditions;
import com.laomei.zhuque.util.PlaceholderParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Map;

/**
 * process sql with context;
 * @author luobo
 **/
public class SqlTransform implements Transform {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlTransform.class);

    private String sqlWithPlaceholder;

    private JdbcTemplate jdbcTemplate;

    public SqlTransform(JdbcTemplate jdbcTemplate, String sqlWithPlaceholder) {
        Preconditions.checkNotNull(jdbcTemplate);
        Preconditions.checkNotNull(sqlWithPlaceholder);
        this.jdbcTemplate = jdbcTemplate;
        this.sqlWithPlaceholder = sqlWithPlaceholder;
    }

    @Override
    public Map<String, Object> transform(Map<String, Object> context) {
        Preconditions.checkNotNull(context);
        return doSqlTransAndAddToContextWithPrefix(context);
    }

    private Map<String, Object> doSqlTransAndAddToContextWithPrefix(Map<String, Object> context) {
        String sql = replacePlaceholderInSqlWithContext(context, sqlWithPlaceholder);
        return processSqlWithJdbcTemplate(sql);
    }

    private Map<String, Object> processSqlWithJdbcTemplate(String sql) {
        try {
            return jdbcTemplate.queryForMap(sql);
        } catch (Exception e) {
            LOGGER.debug("process sql failed. SQL: {}", sql, e);
            return null;
        }
    }

    private String replacePlaceholderInSqlWithContext(Map<String, Object> context, String sqlWithPlaceholder) {
        PlaceholderParser placeholderParser = PlaceholderParser.getParser(context);
        return placeholderParser.replacePlaceholder(sqlWithPlaceholder);
    }
}
