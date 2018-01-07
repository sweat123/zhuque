package com.laomei.zhuque.core.executor;

import java.util.List;
import java.util.Map;

/**
 * @author luobo
 **/
public class NoopExecutor implements Executor {

    @Override
    public void execute(List<Map<String, Object>> contexts) {
    }

    @Override
    public void close() {

    }
}
