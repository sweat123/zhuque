package com.laomei.zhuque.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author luobo
 **/
public class Scheduler implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Scheduler.class);

    private Collector kafkaCollector;

    private Processor processor;

    private AtomicBoolean isStart;

    private AtomicBoolean isClose;

    public Scheduler(Collector kafkaCollector, Processor processor) {
        this.kafkaCollector = kafkaCollector;
        this.processor = processor;
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
            processor.process(kafkaRecords);
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
        try {
            processor.close();
        } catch (Exception e) {
            LOGGER.error("close processor failed.", e);
        }
        LOGGER.info("PreProcessor is closed...");
    }
}
