package com.laomei.zhuque.core.executor;

import com.laomei.zhuque.core.Context;

import java.util.Collection;

/**
 * @author luobo
 */
public interface Executor {

    void execute(Collection<Context> contexts);

    void close();
}
