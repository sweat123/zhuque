package com.laomei.zhuque.core.executor;

import com.google.common.base.Preconditions;
import com.laomei.zhuque.core.reducer.Reducer;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author luobo
 **/
public class NoopExecutor implements Executor {

    private AtomicBoolean isClosed;

    private Reducer reducer;

    public NoopExecutor(Reducer reducer) {
        Preconditions.checkNotNull(reducer);
        this.reducer = reducer;
        isClosed = new AtomicBoolean(false);
    }

    @Override
    public void execute(Collection<Map<String, Object>> contexts) {
        if (!contexts.isEmpty()) {
            if (isClosed.get()) return;
            reducer.reduce(contexts);
        }
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            reducer.close();
        }
    }
}
