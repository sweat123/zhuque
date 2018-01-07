package com.laomei.zhuque.core.executor;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;

/**
 * @author luobo
 **/
public class SqlExecutor implements Executor {

    private Disruptor<MsgEntry> disruptor;

    private RingBuffer<MsgEntry> ringBuffer;

    private EventTranslatorOneArg<MsgEntry, List<Map<String, Object>>> TRANSLATOR =
            (event, sequence, arg0) -> {
                event.contexts = arg0;
            };

    public SqlExecutor() {
        disruptor = new Disruptor<>(new MsgEntryFactory(), 4096, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "SQL-executor-thread");
            }
        });
        disruptor.handleEventsWith(new MsgEntryHandler(this));
        ringBuffer = disruptor.getRingBuffer();
        disruptor.start();
    }

    @Override
    public void execute(List<Map<String, Object>> contexts) {
        ringBuffer.publishEvent(TRANSLATOR, contexts);
    }

    @Override
    public void close() {
        disruptor.shutdown();
    }

    private void executeSqls(List<Map<String, Object>> contexts) {

    }

    static class MsgEntry {
        List<Map<String, Object>> contexts;

        MsgEntry(List<Map<String, Object>> contexts) {
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
            sqlExecutor.executeSqls(event.contexts);
            event.contexts = null;
        }
    }
}
