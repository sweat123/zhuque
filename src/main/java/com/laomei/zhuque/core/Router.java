package com.laomei.zhuque.core;

import java.util.Collection;

/**
 * @author luobo
 */
public interface Router {

    void registry(String task);

    boolean deleteTask(String task);

    Collection<String> allTasks();
}
