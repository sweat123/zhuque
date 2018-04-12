package com.laomei.zhuque.core.transformation;

import com.google.common.base.Preconditions;
import com.laomei.zhuque.core.Context;
import com.laomei.zhuque.util.PlaceholderParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Map;

/**
 * process sql with context;
 * SqlTransform and PlaceholderTransform can't be exist together;
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
    public Context transform(Context context) {
        Preconditions.checkNotNull(context);
        return doSqlTransAndAddToContextWithPrefix(context);
    }

    private Context doSqlTransAndAddToContextWithPrefix(Context context) {
        String sql = replacePlaceholderInSqlWithContext(context, sqlWithPlaceholder);
        try {
            Map<String, Object> result = jdbcTemplate.queryForMap(sql);
            Context newCtx = Context.emptyCtx(context);
            newCtx.putAll(result);
            return newCtx;
        } catch (Exception e) {
            LOGGER.debug("process sql failed. SQL: {}", sql, e);
            return null;
        }
    }

    private String replacePlaceholderInSqlWithContext(Context context, String sqlWithPlaceholder) {
        PlaceholderParser placeholderParser = PlaceholderParser.getParser(context.getUnmodifiableCtx());
        return placeholderParser.replacePlaceholder(sqlWithPlaceholder);
    }
}
