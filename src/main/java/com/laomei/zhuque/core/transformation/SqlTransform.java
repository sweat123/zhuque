package com.laomei.zhuque.core.transformation;

import com.google.common.base.Preconditions;

import java.util.Map;

/**
 * @author luobo
 **/
public class SqlTransform implements Transform {

    private String sql;

    private String prefix;

    public SqlTransform(String sql, String prefix) {
        Preconditions.checkNotNull(sql);
        Preconditions.checkNotNull(prefix);
        this.sql = sql;
        this.prefix = prefix;
    }

    @Override
    public Map<String, Object> transform(Map<String, Object> context) {
        Preconditions.checkNotNull(context);
        return null;
    }
}
