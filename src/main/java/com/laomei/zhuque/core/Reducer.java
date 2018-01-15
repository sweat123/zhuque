package com.laomei.zhuque.core;

import java.util.Collection;
import java.util.Map;

/**
 * @author luobo
 */
public interface Reducer {
    void reduce(Collection<Map<String, Object>> contexts);

    void close();
}
