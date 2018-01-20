package com.laomei.zhuque.core;

import com.laomei.zhuque.config.ZqInstanceFactory;
import com.laomei.zhuque.core.executor.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private AtomicBoolean isStart;

    private AtomicBoolean isClose;

    public static Scheduler newScheduler(SyncAssignment assignment, ZqInstanceFactory factory) {
        KafkaCollector collector = new KafkaCollector(factory.kafkaConsumer());
        //TODO: set transforms tag before transform configuration in temp.yml
        //init transforms
        return null;
    }

    private Scheduler(Collector kafkaCollector, Processor processor, Executor executor) {
        this.kafkaCollector = kafkaCollector;
        this.processor = processor;
        this.executor = executor;
        isStart = new AtomicBoolean(false);
        isClose = new AtomicBoolean(false);
    }

    public void start() {
        if (!isStart.compareAndSet(false, true)) {
            LOGGER.info("Scheduler is running.");
            return;
        }
        LOGGER.info("Scheduler start...");
        while (!isClose.get()) {
            List<KafkaRecord> kafkaRecords = kafkaCollector.collect();
            List<Map<String, Object>> results = processor.process(kafkaRecords);
            executor.execute(results);
        }
    }

    @Override
    public void close() {
        if (!isClose.compareAndSet(false, true)) {
            LOGGER.error("Scheduler is closing...");
            return;
        }
        LOGGER.info("Scheduler begin to close...");
        kafkaCollector.close();
        processor.close();
        executor.close();
        LOGGER.info("Scheduler is closed...");
    }
}
