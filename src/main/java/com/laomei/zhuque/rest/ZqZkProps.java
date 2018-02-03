package com.laomei.zhuque.rest;

/**
 * @author luobo on 2018/2/3 15:59
 */
public interface ZqZkProps {

    public static interface Path {
        public static String ZHU_QUE_ROOT_NODE = "/zhuque";
        public static String ZHU_QUE_TASKS_NODE = "/zhuque/tasks";
    }

    public static interface AssignmentState {
        public static String RUNNING = "running";
        public static String NOT_RUNNING = "not_running";
        public static String WAIT_FOR_CLOSE = "wait_for_close";
    }
}
