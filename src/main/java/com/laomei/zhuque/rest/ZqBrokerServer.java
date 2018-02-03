package com.laomei.zhuque.rest;

import com.laomei.zhuque.config.ZkProperties;
import com.laomei.zhuque.core.SyncAssignment;
import com.laomei.zhuque.util.ZkUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Collections;
import java.util.List;


/**
 * @author luobo on 2018/2/3 13:02
 */
@NotThreadSafe
@Component
public class ZqBrokerServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZqBrokerServer.class);

    private CuratorFramework zkClient;

    private Validator validator;

    public ZqBrokerServer(ZkProperties zkProperties) {
        if (zkProperties.getZkUrl() == null) {
            throw new NullPointerException("zk host can't be null;");
        }
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder().connectString(zkProperties.getZkUrl());
        if (zkProperties.getSessionTimeoutMs() != null) {
            builder.sessionTimeoutMs(zkProperties.getSessionTimeoutMs());
        }
        if (zkProperties.getConnectTimeoutMs() != null) {
            builder.connectionTimeoutMs(zkProperties.getConnectTimeoutMs());
        }
        if (zkProperties.getRetryTimes() != null && zkProperties.getBaseSleepTimeMs() != null) {
            builder.retryPolicy(new ExponentialBackoffRetry(zkProperties.getBaseSleepTimeMs(), zkProperties.getRetryTimes()));
        }
        zkClient = builder.build();
        validator = getAssignmentValidator();
    }

    /**
     * start broker server; We will create root node and tasks node if they are not exist;
     */
    public void start() {
        String rootNodePath = ZqZkProps.Path.ZHU_QUE_ROOT_NODE;
        String taskNodePath = ZqZkProps.Path.ZHU_QUE_TASKS_NODE;
        if (!ZkUtil.ensurePath(zkClient, rootNodePath)) {
            ZkUtil.createPersistentPathWithoutParent(zkClient, rootNodePath);
        }
        if (!ZkUtil.ensurePath(zkClient, taskNodePath)) {
            ZkUtil.createPersistentPathWithoutParent(zkClient, taskNodePath);
        }
        LOGGER.info("ZhuQue broker server start...");
    }

    public List<String> getAllAssignments() {
        String taskNodePath = ZqZkProps.Path.ZHU_QUE_TASKS_NODE;
        List<String> assignments = ZkUtil.getAllChildren(zkClient, taskNodePath);
        return assignments == null ? Collections.emptyList() : assignments;
    }

    public boolean postAssignment(String assignmentName, String assignmentConfiguration) {
        if (!validator.validate(assignmentConfiguration)) {
            return false;
        }
        //create node /zhuque/tasks/xxx
        String assignmentNodePath = ZkUtil.mergePathWith(ZqZkProps.Path.ZHU_QUE_TASKS_NODE, assignmentName);
        if (ZkUtil.ensurePath(zkClient, assignmentNodePath)) {
            LOGGER.error("assignment {} is already exist;", assignmentName);
            return false;
        }
        LOGGER.info("create assignment {} node succeed;", assignmentName);
        return ZkUtil.createPersistentPathWithoutParent(zkClient, assignmentNodePath);
    }

    public boolean deleteAssignment(String assignmentName) {
        String assignmentNodePath = ZkUtil.mergePathWith(ZqZkProps.Path.ZHU_QUE_TASKS_NODE, assignmentName);
        if (!ZkUtil.ensurePath(zkClient, assignmentNodePath)) {
            LOGGER.error("assignment {} not exist;", assignmentName);
            return false;
        }
        //before remove assignment node, we should set lock data WAIT_FOR_CLOSE;
        ZkUtil.ZkLock.setLockData(zkClient, assignmentNodePath, ZqZkProps.AssignmentState.WAIT_FOR_CLOSE.getBytes());
        //remove lock & assignment node;
        return ZkUtil.deletePathWithChildren(zkClient, assignmentNodePath);
    }

    private Validator getAssignmentValidator() {
        return assignment -> {
            try {
                SyncAssignment.newSyncTaskMetadata(assignment);
                return true;
            } catch (Exception ignore) {
                return false;
            }
        };
    }

    @FunctionalInterface
    private interface Validator {

        /**
         * validate assignment configuration
         * @param assignment assignment string
         * @return true if the assignment configuration is valid;
         */
        boolean validate(String assignment);
    }
}
