package com.laomei.zhuque.rest;

import com.laomei.zhuque.config.ZkProperties;
import com.laomei.zhuque.config.ZqInstanceFactory;
import com.laomei.zhuque.core.Scheduler;
import com.laomei.zhuque.core.SyncAssignment;
import com.laomei.zhuque.exception.BrokerClientRegistryException;
import com.laomei.zhuque.exception.InitSchemaFailedException;
import com.laomei.zhuque.exception.NullReducerClazzException;
import com.laomei.zhuque.exception.UnknownReducerClazzException;
import com.laomei.zhuque.util.StrUtil;
import com.laomei.zhuque.util.ZkUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author luobo on 2018/2/3 13:07
 */
@Component
public class ZqBrokerClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZqBrokerClient.class);

    private CuratorFramework zkClient;

    private String tasksRootNode;

    private ZqInstanceFactory factory;

    private ConcurrentHashMap<String, Scheduler> schedulers;

    public ZqBrokerClient(ZkProperties zkProperties, ZqInstanceFactory factory) {
        this.tasksRootNode = ZqZkProps.Path.ZHU_QUE_TASKS_NODE;
        this.schedulers = new ConcurrentHashMap<>();
        this.factory = factory;
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
    }

    public void registry() throws BrokerClientRegistryException {
        String taskNodePath = ZqZkProps.Path.ZHU_QUE_TASKS_NODE;
        if (!ZkUtil.ensurePath(zkClient, taskNodePath)) {
            ZkUtil.createPersistentPathWithParent(zkClient, taskNodePath);
        }
        LOGGER.info("ZhuQue broker client start...");
        PathChildrenCache cache = new PathChildrenCache(zkClient, tasksRootNode, true);
        try {
            cache.start();
        } catch (Exception e) {
            LOGGER.error("pathChildrenCache start failed. node path: {}", tasksRootNode, e);
            try {
                cache.close();
            } catch (Exception ignore) { }
            throw new BrokerClientRegistryException("pathChildrenCache start failed. node path: tasksRootNode", e);
        }
        cache.getListenable().addListener((client, event) -> {
            switch (event.getType()) {
            case CHILD_ADDED:
                //new assignment is added
                tryLockAndStartAssignment(client, event.getData().getPath(), event.getData().getData());
            case CHILD_UPDATED:
                //assignment is updated
            case CHILD_REMOVED:
                //assignment is deleted
            default: break;
            }
        });
    }

    private void tryLockAndStartAssignment(CuratorFramework zkClient, String path, byte[] data) {
        tryLockAndStartAssignment(zkClient, path, data, false);
    }

    private void tryLockAndStartAssignment(CuratorFramework zkClient, String path, byte[] data, boolean isLoad) {
        if (isLoad && ZkUtil.ZkLock.ensureLock(zkClient, path)) {
            //There is no task without lock when program startup;
            return;
        }
        listenToAssignment(zkClient, path);
        lockAndCreateAssignment(zkClient, path, data);
    }

    private void lockAndCreateAssignment(CuratorFramework zkClient, String path, byte[] data) {
        byte[] state = ZqZkProps.AssignmentState.RUNNING.getBytes();
        boolean locked = ZkUtil.ZkLock.addLock(zkClient, path, state);
        if (!locked) {
            return;
        }
        String taskConfigStr = new String(data);
        String taskName = StrUtil.subStrAfterLastAssignChar(path, '/');
        createScheduler(taskName, taskConfigStr);
    }

    private void listenToAssignment(CuratorFramework zkClient, String path) {
        PathChildrenCache cache = new PathChildrenCache(zkClient, path, true);
        try {
            cache.start();
        } catch (Exception e) {
            LOGGER.error("start pathChildCache failed. node path: {}", path, e);
            return;
        }
        cache.getListenable().addListener((client, event) -> {
            switch (event.getType()) {
            case CHILD_REMOVED:
                //lock is removed
                tryRestartAssignment(client, event.getData().getPath(), event.getData().getData());
            case CHILD_UPDATED:
                //data in lock node is changed
            default: break;
            }
        });
    }

    private void tryRestartAssignment(CuratorFramework zkClient, String lockPath, byte[] data) {
        if (isDeleteAssignment(data)) {
            //delete assignment; we don't need to restart assignment
            return;
        }
        String path = StrUtil.subStrBeforeLastAssignChar(lockPath, '/');
        String assignmentName = StrUtil.subStrAfterLastAssignChar(lockPath, '/');

        // |-------------------------|
        // | 1. stop scheduler       |
        // | 2. create new scheduler |
        // | 3. start scheduler      |
        // |-------------------------|

        removeTaskFromContainerAndStopTask(assignmentName);

        byte[] state = ZqZkProps.AssignmentState.RUNNING.getBytes();
        boolean locked = ZkUtil.ZkLock.addLock(zkClient, path, state);
        if (!locked) {
            return;
        }
        byte[] assignmentBytes = ZkUtil.getNodeData(zkClient, path);
        if (assignmentBytes == null) {
            LOGGER.error("Get data for path {} failed, restart task failed.", path);
            return;
        }
        String assignment = new String(assignmentBytes);
        createScheduler(assignmentName, assignment);
    }

    private void createScheduler(String name, String assignmentStr) {
        SyncAssignment assignment = SyncAssignment.newSyncTaskMetadata(assignmentStr);
        Scheduler scheduler = null;
        try {
            scheduler = Scheduler.newScheduler(assignment, factory);
        } catch (InitSchemaFailedException e) {
            LOGGER.error("init schema failed; assignment name: {}", name, e);
            return;
        } catch (UnknownReducerClazzException e) {
            LOGGER.error("reducer clazz configure error;", e);
            return;
        } catch (NullReducerClazzException e) {
            LOGGER.error(e.getMessage(), e);
            return;
        }
        scheduler.start();
        addTaskInContainerWithTaskName(name, scheduler);
    }

    private void addTaskInContainerWithTaskName(String taskName, Scheduler scheduler) {
        schedulers.put(taskName, scheduler);
    }

    private void removeTaskFromContainerAndStopTask(String name) {
        Scheduler scheduler = schedulers.get(name);
        if (scheduler != null) {
            scheduler.close();
            schedulers.remove(name);
        }
    }

    private boolean isDeleteAssignment(byte[] data) {
        return ZqZkProps.AssignmentState.WAIT_FOR_CLOSE.equals(new String(data));
    }
}
