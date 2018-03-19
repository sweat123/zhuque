package com.laomei.zhuque.rest;

import com.laomei.zhuque.config.ZkProperties;
import com.laomei.zhuque.core.SyncAssignment;
import com.laomei.zhuque.util.JsonUtil;
import com.laomei.zhuque.util.ZkUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Collections;
import java.util.List;

import static com.laomei.zhuque.rest.ZqZkProps.AssignmentState.STATE;


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
        zkClient.start();
        validator = getAssignmentValidator();
    }

    /**
     * start broker server; We will create root node and tasks node if they are not exist;
     */
    public void start() {
        String taskNodePath = ZqZkProps.Path.ZHU_QUE_TASKS_NODE;
        if (!ZkUtil.ensurePath(zkClient, taskNodePath)) {
            ZkUtil.createPersistentPathWithParent(zkClient, taskNodePath);
        }
        LOGGER.info("ZhuQue broker server start...");
    }

    public List<String> getAllAssignments() {
        String taskNodePath = ZqZkProps.Path.ZHU_QUE_TASKS_NODE;
        List<String> assignments = ZkUtil.getChildren(zkClient, taskNodePath);
        return assignments == null ? Collections.emptyList() : assignments;
    }

    public String getAssignment(String name) {
        String taskNodePath = ZkUtil.mergePathWith(ZqZkProps.Path.ZHU_QUE_TASKS_NODE, name);
        byte[] nodeData = ZkUtil.getNodeData(zkClient, taskNodePath);
        return nodeData == null ? null : new String(nodeData);
    }

    public synchronized boolean postAssignment(String assignmentName, String assignment) throws KeeperException.NodeExistsException {
        if (!validator.validate(assignment)) {
            return false;
        }
        //create node /zhuque/tasks/xxx
        String assignmentNodePath = ZkUtil.mergePathWith(ZqZkProps.Path.ZHU_QUE_TASKS_NODE, assignmentName);
        if (ZkUtil.ensurePath(zkClient, assignmentNodePath)) {
            LOGGER.error("assignment {} is already exist;", assignmentName);
            throw new KeeperException.NodeExistsException("assignment '" + assignmentName + "' not exist;");
        }
        LOGGER.info("create assignment {} node succeed;", assignmentName);
        return ZkUtil.createPersistentPathWithoutParent(zkClient, assignmentNodePath, assignment.getBytes());
    }

    public synchronized boolean deleteAssignment(String assignmentName) throws KeeperException.NoNodeException {
        String assignmentNodePath = ZkUtil.mergePathWith(ZqZkProps.Path.ZHU_QUE_TASKS_NODE, assignmentName);
        if (!ZkUtil.ensurePath(zkClient, assignmentNodePath)) {
            LOGGER.error("assignment {} not exist;", assignmentName);
            throw new KeeperException.NoNodeException("assignment '" + assignmentName + "' not exist;");
        }
        //before remove assignment node, we should set lock data WAIT_FOR_CLOSE;
        byte[] state = JsonUtil.convertObjToJsonByteArr(STATE, ZqZkProps.AssignmentState.WAIT_FOR_CLOSE);
        ZkUtil.ZkLock.setLockData(zkClient, assignmentNodePath, state);
        //remove lock & assignment node;
        return ZkUtil.deletePathWithChildren(zkClient, assignmentNodePath);
    }

    public synchronized boolean updateAssignment(String assignmentName, String assignment) throws KeeperException.NoNodeException {
        String assignmentNodePath = ZkUtil.mergePathWith(ZqZkProps.Path.ZHU_QUE_TASKS_NODE, assignmentName);
        if (!ZkUtil.ensurePath(zkClient, assignmentNodePath)) {
            LOGGER.error("assignment {} not exist;", assignmentName);
            throw new KeeperException.NoNodeException("assignment '" + assignmentName + "' not exist;");
        }
        return ZkUtil.setNodeData(zkClient, assignmentNodePath, assignment.getBytes());
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
