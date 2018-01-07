package com.laomei.zhuque.core.executor;

import java.util.List;
import java.util.Map;

/**
 * @author luobo
 */
public interface Executor {

    void execute(List<Map<String, Object>> contexts);

    void close();
}
