package com.laomei.zhuque.core.reducer;

import com.laomei.zhuque.util.ObjTypeUtil;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author luobo
 **/
public class MySqlSchemaHelper implements SchemaHelper {
    private static final String MYSQL_DESC_TABLE_TEMPLATE = "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_DEFAULT, " +
            "IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s'";
    private static final String COLUMN_NAME = "COLUMN_NAME";
    private static final String DATA_TYPE = "DATA_TYPE";
    private JdbcTemplate jdbcTemplate;

    private String sqlForSchemas;

    public MySqlSchemaHelper(JdbcTemplate jdbcTemplate, String tableName) {
        this.jdbcTemplate = jdbcTemplate;
        this.sqlForSchemas = String.format(MYSQL_DESC_TABLE_TEMPLATE, tableName);
    }

    @Override
    public void init() {
    }

    @Override
    public Map<String, Class<?>> getSchema() {
        List<Map<String, Object>> results = jdbcTemplate.queryForList(sqlForSchemas);
        return getTableSchemas(results);
    }

    private Map<String, Class<?>> getTableSchemas(List<Map<String, Object>> results) {
        Map<String, Class<?>> metadatas = new HashMap<>(results.size());
        results.forEach(result -> {
            String colName = (String) result.get(COLUMN_NAME);
            Class<?> clazz = ObjTypeUtil.getDataType(String.valueOf(result.get(DATA_TYPE)));
            metadatas.put(colName, clazz);
        });
        return metadatas;
    }
}