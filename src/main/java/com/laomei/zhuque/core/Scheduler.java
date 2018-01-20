package com.laomei.zhuque.core;

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

    private Processor preProcessor;

    private Executor executor;

    private AtomicBoolean isStart;

    private AtomicBoolean isClose;

    public Scheduler(Collector kafkaCollector, Processor preProcessor, Executor executor) {
        this.kafkaCollector = kafkaCollector;
        this.preProcessor = preProcessor;
        this.executor = executor;
        isStart = new AtomicBoolean(false);
        isClose = new AtomicBoolean(false);
    }

    public void start() {
        if (!isStart.compareAndSet(false, true)) {
            LOGGER.info("PreProcessor is running.");
            return;
        }
        LOGGER.info("PreProcessor start...");
        while (!isClose.get()) {
            List<KafkaRecord> kafkaRecords = kafkaCollector.collect();
            List<Map<String, Object>> resutls = preProcessor.process(kafkaRecords);
            executor.execute(resutls);
        }
    }

    @Override
    public void close() {
        if (!isClose.compareAndSet(false, true)) {
            LOGGER.error("PreProcessor is closing...");
            return;
        }
        LOGGER.info("PreProcessor begin to close...");
        kafkaCollector.close();
        preProcessor.close();
        executor.close();
        LOGGER.info("PreProcessor is closed...");
    }
}
