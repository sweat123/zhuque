package com.laomei.zhuque.core.executor;

import com.google.common.base.Preconditions;
import com.laomei.zhuque.core.reducer.Reducer;

import java.util.Collection;
import java.util.Map;

/**
 * @author luobo
 **/
public class NoopExecutor implements Executor {

    private Reducer reducer;

    public NoopExecutor(Reducer reducer) {
        Preconditions.checkNotNull(reducer);
        this.reducer = reducer;
    }

    @Override
    public void execute(Collection<Map<String, Object>> contexts) {
        if (!contexts.isEmpty()) {
            reducer.reduce(contexts);
        }
    }

    @Override
    public void close() {
        reducer.close();
    }
}
