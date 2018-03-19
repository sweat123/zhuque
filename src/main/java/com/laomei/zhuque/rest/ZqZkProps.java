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
        public static String STATE = "state";
        public static String RUNNING = "running";
        public static String NOT_RUNNING = "not_running";
        //task will be closed soon;
        public static String WAIT_FOR_CLOSE = "wait_for_close";
        //assignment need reread and task should be restart;
        public static String NEED_UPDATE = "need_update";
    }
}
