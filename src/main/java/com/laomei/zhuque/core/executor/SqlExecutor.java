package com.laomei.zhuque.core.executor;

import com.google.common.base.Preconditions;
import com.laomei.zhuque.core.Context;
import com.laomei.zhuque.core.reducer.Reducer;
import com.laomei.zhuque.core.SyncAssignment.SyncAssignmentProcessor.EntitySql;
import com.laomei.zhuque.exception.NoResultException;
import com.laomei.zhuque.util.PlaceholderParser;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author luobo
 **/
public class SqlExecutor implements Executor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlExecutor.class);

    private AtomicBoolean isClosed;

    private Reducer reducer;

    private Collection<EntitySql> entitySqlList;

    private JdbcTemplate jdbcTemplate;

    private Disruptor<MsgEntry> disruptor;

    private RingBuffer<MsgEntry> ringBuffer;

    private EventTranslatorOneArg<MsgEntry, Collection<Context>> TRANSLATOR =
            (event, sequence, arg0) -> {
                event.contexts = arg0;
            };

    public SqlExecutor(Collection<EntitySql> entitySqlList, JdbcTemplate jdbcTemplate, Reducer reducer) {
        Preconditions.checkNotNull(entitySqlList, "entity sql list can't be null in SQL executor");
        Preconditions.checkNotNull(jdbcTemplate);
        Preconditions.checkNotNull(reducer);
        this.entitySqlList = entitySqlList;
        this.jdbcTemplate = jdbcTemplate;
        this.reducer = reducer;
        isClosed = new AtomicBoolean(false);
        disruptor = new Disruptor<>(new MsgEntryFactory(), 4096, r -> {
            return new Thread(r, "SQL-executor-thread");
        });
        disruptor.handleEventsWith(new MsgEntryHandler(this));
        ringBuffer = disruptor.getRingBuffer();
        disruptor.start();
    }

    @Override
    public void execute(Collection<Context> contexts) {
        if (isClosed.get()) return;
        ringBuffer.publishEvent(TRANSLATOR, contexts);
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            disruptor.shutdown();
            reducer.close();
            entitySqlList.clear();
            entitySqlList = null;
            jdbcTemplate = null;
            ringBuffer = null;
            disruptor = null;
            reducer = null;
        }
    }

    private void processSqls(Collection<Context> contexts) {
        List<Context> results = new ArrayList<>(contexts.size());
        for (Context context : contexts) {
            if (isClosed.get()) return;
            try {
                Context result = processSql(context);
                results.add(result);
            } catch (NoResultException e) {
                LOGGER.debug("get NoResultException in SqlExecutor; context: {}", context, e);
            }
        }
        if (!results.isEmpty()) {
            reducer.reduce(results);
        }
    }

    private Context processSql(Context context) throws NoResultException {
        if (isClosed.get()) return null;
        Context newContext = Context.copyOf(context);
        for (EntitySql entitySql : entitySqlList) {
            if (isClosed.get()) return null;
            String sqlWithPlaceholder = entitySql.getSql();
            String prefixName = entitySql.getName();
            boolean required = entitySql.getRequired() == null ? false : entitySql.getRequired();
            PlaceholderParser placeholderParser = PlaceholderParser.getParser(newContext.getUnmodifiableCtx());
            String sql = placeholderParser.replacePlaceholder(sqlWithPlaceholder);
            Map<String, Object> result = jdbcTemplate.queryForMap(sql);
            if (result == null && required) {
                throw new NoResultException("the result of sql " + sql + " is null " +
                        "and it's required in configuration");
            }
            newContext = addResultInContext(newContext, result, prefixName);
        }
        return newContext;
    }

    private Context addResultInContext(Context context,
                                       Map<String, Object> result,
                                       String prefixName) {
        for (Map.Entry<String, Object> entry : result.entrySet()) {
            if (isClosed.get()) return null;
            String key = entry.getKey();
            Object value = entry.getValue();
            Map<String, Object> prefixContext = (Map<String, Object>) context.get(prefixName);
            if (prefixContext == null) {
                prefixContext = new HashMap<>();
                context.put(prefixName, prefixContext);
            }
            prefixContext.put(key, value);
        }
        return context;
    }


    // belows are contexts wrapper for disruptor

    static class MsgEntry {
        Collection<Context> contexts;

        MsgEntry(Collection<Context> contexts) {
            this.contexts = contexts;
        }
    }

    static class MsgEntryFactory implements EventFactory<MsgEntry> {
        @Override
        public MsgEntry newInstance() {
            return new MsgEntry(null);
        }
    }

    static class MsgEntryHandler implements EventHandler<MsgEntry> {

        SqlExecutor sqlExecutor;

        MsgEntryHandler(SqlExecutor sqlExecutor) {
            this.sqlExecutor = sqlExecutor;
        }

        @Override
        public void onEvent(MsgEntry event, long sequence, boolean endOfBatch) throws Exception {
            sqlExecutor.processSqls(event.contexts);
            event.contexts = null;
        }
    }
}
