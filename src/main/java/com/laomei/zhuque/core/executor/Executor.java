package com.laomei.zhuque.core.executor;

import java.util.Collection;
import java.util.Map;

/**
 * @author luobo
 */
public interface Executor {

    void execute(Collection<Map<String, Object>> contexts);

    void close();
}
