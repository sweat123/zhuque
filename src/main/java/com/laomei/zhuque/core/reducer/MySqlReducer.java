package com.laomei.zhuque.core.reducer;

import com.laomei.zhuque.core.Context;
import com.laomei.zhuque.core.JdbcTemplateHolder;
import com.laomei.zhuque.core.OffsetCenter;
import com.laomei.zhuque.util.ObjTypeUtil;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @author luobo
 **/
public class MySqlReducer implements Reducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlReducer.class);
    private static final int FIVE_HUNDRED = 500;

    private long upsertCount;

    private final String tableName;

    private final Map<String, Class<?>> fieldClazzMap;

    private JdbcTemplate jdbcTemplate;

    private AtomicBoolean isClosed;

    private String UPSERT_SQL;

    private String[] fields;

    private Disruptor<Msg> disruptor;

    private RingBuffer<Msg> ringBuffer;

    private EventTranslatorOneArg<Msg, Collection<Context>> TRANSLATOR = (event, sequence, arg0) -> {
        event.contexts = arg0;
    };

    public MySqlReducer(
            String tableName,
            String url,
            String username,
            String password) {
        this.upsertCount = 0;
        this.isClosed = new AtomicBoolean(false);
        this.tableName = tableName;

        JdbcTemplateHolder.registry(url, username, password);
        jdbcTemplate = JdbcTemplateHolder.getJdbcTemplate(url);

        fieldClazzMap = new MySqlSchemaHelper(jdbcTemplate, tableName).getSchema();
        UPSERT_SQL = getUpsertSql();
        disruptor = new Disruptor<>(new MsgFactory(), 16384, r -> {
            return new Thread(r, "mysql-reducer-" + tableName + "-disruptor");
        });
        disruptor.handleEventsWith(new ReducerMsgHandler(this));
        ringBuffer = disruptor.getRingBuffer();
        disruptor.start();
    }

    @Override
    public void reduce(Collection<Context> contexts) {
        if (isClosed.get()) {
            return;
        }
        ringBuffer.publishEvent(TRANSLATOR, contexts);
    }

    @Override
    public void close() {
        if (!isClosed.compareAndSet(false, true)) {
            LOGGER.info("MysqlReducer 开始关闭.");
            disruptor.shutdown();
        }
    }

    private void reduceWithMysql(final Collection<Context> contexts) {
        if (isClosed.get()) {
            return;
        }
        batchUpsertContexts(contexts);
    }

    private void batchUpsertContexts(Collection<Context> contexts) {
        //所有需要插入字段数据都放入documents内;
        List<Map<String, Object>> documents = contexts.stream()
                .map(this::getFieldsWithValues)
                .collect(Collectors.toList());
        //TODO: 这里需要判断处理默认值和null值, 现在的处理方式有问题;
        documents.forEach(document -> {
            fieldClazzMap.forEach((field, metadata) -> {
                if (!document.containsKey(field)) {
                    //schema里存在的字段，但是在document里不存在，则在document里添加
                    //这个字段，同时设置为数据库里的默认值(或者null);
                }
            });
        });
        if (isClosed.get()) {
            return;
        }
        int[] v = jdbcTemplate.batchUpdate(UPSERT_SQL, new BatchPreparedStatementSetter() {

            @Override
            public void setValues(final PreparedStatement ps, final int i) throws SQLException {
                Map<String, Object> document = documents.get(i);
                int paramNum = fields.length;
                for (int idx = 0; idx < paramNum; idx++) {
                    Object v = document.get(fields[idx]);
                    ps.setObject(idx + 1, v);
                    ps.setObject(idx + 1 + paramNum, v);
                }
            }

            @Override
            public int getBatchSize() {
                return documents.size();
            }
        });
        upsertCount += contexts.size();
        if (upsertCount % FIVE_HUNDRED == 0) {
            LOGGER.info("表{}已插入大约{}条数据.", tableName, upsertCount);
        }
        if (isClosed.get()) {
            //确保停止时，不提交offset;
            return;
        }
        if (contexts.size() > 0) {
            contexts.forEach(context -> OffsetCenter.submit(context.getTopic(), context.getPartition(), context.getOffset()));
        }
    }

    /**
     * 根据表的schema，得到所有需要被插入到表内的字段以及对应的值
     * @param context context
     * @return 所有要被插入到表的字段和对应的值
     */
    private Map<String, Object> getFieldsWithValues(Context context) {
        Map<String, Object> results = new HashMap<>();
        Map<String, Object> unmodifiedCtxMap = context.getUnmodifiableCtx();
        getFieldsWithContext(results, unmodifiedCtxMap);
        return results;
    }

    private void getFieldsWithContext(Map<String, Object> results, Map<String, Object> ctx) {
        for (Map.Entry<String, Object> entry : ctx.entrySet()) {
            String field = entry.getKey();
            Object value = entry.getValue();
                if (value instanceof Map) {
                getFieldsWithContext(results, (Map<String, Object>) value);
            } else {
                results.put(field, ObjTypeUtil.convert(value, fieldClazzMap.get(field)));
            }
        }
    }

    private String getUpsertSql() {
        fillFields(fieldClazzMap.keySet().iterator());
        StringBuilder builder = new StringBuilder();
        builder = builder.append("INSERT INTO ");
        builder = builder.append(tableName);
        builder = builder.append("(");
        joinToBuilder(builder, ",", fields);
        builder = builder.append(") VALUES(");
        copyToBuilder(builder, ",", "?", fields.length);
        builder = builder.append(") ON DUPLICATE KEY UPDATE ");
        joinKeyToBuilder(builder, ",", fields);
        return builder.toString();
    }

    private void fillFields(Iterator<String> iter) {
        fields = new String[fieldClazzMap.size()];
        int i = 0;
        while (iter.hasNext()) {
            fields[i++] = iter.next();
        }
    }

    private void joinToBuilder(StringBuilder builder, String delim, String[] items) {
        for (int i = 0; i < items.length; i++) {
            if (i > 0) {
                builder.append(delim);
            }
            builder.append(items[i]);
        }
    }

    private void copyToBuilder(StringBuilder builder, String delim, String item, int n) {
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                builder.append(delim);
            }
            builder.append(item);
        }
    }

    private void joinKeyToBuilder(StringBuilder builder, String delim, String[] fields) {
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) {
                builder.append(delim);
            }
            String key = fields[i];
            builder.append(key).append("=").append('?');
        }
    }

    static class Msg {
        Collection<Context> contexts;

        Msg(List<Context> contexts) {
            this.contexts = contexts;
        }
    }

    static class MsgFactory implements EventFactory<Msg> {
        @Override
        public Msg newInstance() {
            return new Msg(null);
        }
    }

    static class ReducerMsgHandler implements EventHandler<Msg> {

        MySqlReducer reducer;

        ReducerMsgHandler(MySqlReducer reducer) {
            this.reducer = reducer;
        }

        @Override
        public void onEvent(final Msg event, final long sequence, final boolean endOfBatch) throws Exception {
            reducer.reduceWithMysql(event.contexts);
            event.contexts = null;
        }
    }
}