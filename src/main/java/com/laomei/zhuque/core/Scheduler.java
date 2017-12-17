package com.laomei.zhuque.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author luobo
 **/
@Component
public class Scheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(Scheduler.class);

    @Autowired
    @Qualifier(value = "preProcessKafkaCollector")
    private Collector kafkaCollector;

    private AtomicBoolean isStart;

    private AtomicBoolean isClose;

    public Scheduler() {
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
            //send kafka records with route
        }
    }

    public void close() {
        if (!isClose.compareAndSet(false, true)) {
            LOGGER.error("PreProcessor is closing...");
            return;
        }
        LOGGER.info("PreProcessor begin to close...");
    }
}
