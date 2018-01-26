package com.laomei.zhuque.core;

import com.laomei.zhuque.config.ZqInstanceFactory;
import com.laomei.zhuque.core.SyncAssignment.SyncAssignmentProcessor.EntitySql;
import com.laomei.zhuque.core.executor.Executor;
import com.laomei.zhuque.core.executor.NoopExecutor;
import com.laomei.zhuque.core.executor.SqlExecutor;
import com.laomei.zhuque.core.reducer.Reducer;
import com.laomei.zhuque.core.reducer.SolrDeleteReducer;
import com.laomei.zhuque.core.reducer.SolrUpdateReducer;
import com.laomei.zhuque.exception.InitSchemaFailedException;
import com.laomei.zhuque.exception.NullReducerClazzException;
import com.laomei.zhuque.exception.UnknownReducerClazzException;
import org.apache.solr.client.solrj.SolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author luobo
 **/
public class Scheduler implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Scheduler.class);

    private Collector kafkaCollector;

    private Processor processor;

    private Executor executor;

    private AtomicBoolean isClose;

    public static Scheduler newScheduler(SyncAssignment assignment, ZqInstanceFactory factory)
            throws InitSchemaFailedException, UnknownReducerClazzException, NullReducerClazzException {
        KafkaCollector collector = new KafkaCollector(factory.kafkaConsumer());
        JdbcTemplate jdbcTemplate = factory.jdbcTemplate(assignment.getMysql());
        KafkaRecordProcessor processor = new KafkaRecordProcessor(assignment.getProcessor().getTopicConfigs(),
                                                                  jdbcTemplate);
        Reducer reducer = getReducerWithAssignment(assignment, factory.solrClient());
        Executor executor = null;
        List<EntitySql> entitySqls = assignment.getProcessor().getEntitySqls();
        if (entitySqls != null && !entitySqls.isEmpty()) {
            executor = new SqlExecutor(entitySqls, jdbcTemplate, reducer);
        } else {
            executor = new NoopExecutor(reducer);
        }
        return new Scheduler(collector, processor, executor);
    }

    /**
     * init reducer;
     * @param assignment assignment
     * @param solrClient SolrClient
     * @return Reducer
     * @throws InitSchemaFailedException init solr collection schemas failed
     * @throws UnknownReducerClazzException the assignment of reducer clazz is not correct
     * @throws NullReducerClazzException the assignment of reducer clazz can't be empty
     */
    private static Reducer getReducerWithAssignment(SyncAssignment assignment, SolrClient solrClient)
            throws InitSchemaFailedException, UnknownReducerClazzException, NullReducerClazzException {
        String clazz = assignment.getProcessor().getReducerClazz();
        if (clazz != null) {
            switch (clazz) {
            case Reducer.SOLR_UPDATE_CLAZZ:
                return new SolrUpdateReducer(assignment.getProcessor().getSolrCollection(), solrClient);
            case Reducer.SOLR_DELETE_CLAZZ:
                return new SolrDeleteReducer(assignment.getProcessor().getSolrCollection(), solrClient);
            default:
                throw new UnknownReducerClazzException("clazz: " + clazz + " is not support in ZhuQue; " +
                        "You should check your reducer clazz configuration;");
            }
        }
        //reducer class can't be empty
        throw new NullReducerClazzException("reducer class in ZhuQue can't be empty;");
    }

    private Scheduler(Collector kafkaCollector, Processor processor, Executor executor) {
        this.kafkaCollector = kafkaCollector;
        this.processor = processor;
        this.executor = executor;
        isClose = new AtomicBoolean(false);
    }

    public void start() {
        try {
            LOGGER.info("Scheduler start...");
            while (!isClose.get()) {
                List<KafkaRecord> kafkaRecords = kafkaCollector.collect();
                if (isClose.get()) {
                    return;
                }
                List<Map<String, Object>> results = processor.process(kafkaRecords);
                if (isClose.get()) {
                    return;
                }
                if (results != null && !results.isEmpty()) {
                    executor.execute(results);
                }
            }
        } catch (Exception e) {
            LOGGER.error("catch an unknown exception; " + e);
            isClose.set(true);
        } finally {
            kafkaCollector.close();
            processor.close();
            executor.close();
            LOGGER.info("Scheduler is closed...");
        }
    }

    @Override
    public void close() {
        if (isClose.compareAndSet(false, true)) {
            LOGGER.info("Scheduler begin to close...");
        }
    }
}
