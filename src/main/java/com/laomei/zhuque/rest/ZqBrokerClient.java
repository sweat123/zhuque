package com.laomei.zhuque.rest;

import com.laomei.zhuque.config.ZkProperties;
import com.laomei.zhuque.config.ZqInstanceFactory;
import com.laomei.zhuque.core.Scheduler;
import com.laomei.zhuque.core.SyncAssignment;
import com.laomei.zhuque.exception.BrokerClientRegistryException;
import com.laomei.zhuque.exception.InitSchemaFailedException;
import com.laomei.zhuque.exception.NullReducerClazzException;
import com.laomei.zhuque.exception.UnknownReducerClazzException;
import com.laomei.zhuque.util.JsonUtil;
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

import static com.laomei.zhuque.rest.ZqZkProps.AssignmentState.STATE;

/**
 * lock node state topology
 *
 * |========================================|
 * |                                        |
 * | RUNNING ===> NEED_UPDATE ===> RUNNING  |
 * |    ||  \\                     /\       |
 * |    ||    \\                  //        |
 * |    ||      \\               //         |
 * |    \/        \\            //          |
 * | WAIT_FOR_CLOSE \\         //           |
 * |                  \\      //            |
 * |                   \/    //             |
 * |                  NOT_RUNNING           |
 * |                                        |
 * |========================================|
 * @author luobo on 2018/2/3 13:07
 */
@Component
public class ZqBrokerClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZqBrokerClient.class);

    private CuratorFramework zkClient;

    private String tasksRootNode;

    private ZqInstanceFactory factory;

    private ConcurrentHashMap<String, Scheduler> schedulers;

    private ConcurrentHashMap<String, PathChildrenCache> pathChildCacheMap;

    public ZqBrokerClient(ZkProperties zkProperties, ZqInstanceFactory factory) {
        this.tasksRootNode = ZqZkProps.Path.ZHU_QUE_TASKS_NODE;
        this.schedulers = new ConcurrentHashMap<>();
        this.pathChildCacheMap = new ConcurrentHashMap<>();
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
        zkClient.start();
    }

    public void start() throws BrokerClientRegistryException {
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
                tryLockAndStartAssignment(client, event.getData().getPath(), event.getData().getData());
                break;
            case CHILD_UPDATED:
                updateAssignment(client, event.getData().getPath());
                break;
            case CHILD_REMOVED:
                deleteAssignment(client, event.getData().getPath());
                break;
            default: break;
            }
        });
        addPathChildCacheInContainerWithPath(tasksRootNode, cache);
    }

    private void updateAssignment(CuratorFramework zkClient, String path) {
        // -------------------------------------------------------------------------------------------------
        // | 1. only need set lock data NEED_UPDATE;                                                       |
        // | 2. PathCacheListener at lock node will catch lock data update event;                          |
        // | 3. check the data in lock node, If state equals NEED_UPDATE, then lock node will be released; |
        // | 4. when lock is released, all healthy broker will try lock assignment;                        |
        // | 5. now new task is started;                                                                   |
        // -------------------------------------------------------------------------------------------------
        byte[] data = JsonUtil.convertObjToJsonByteArr(STATE, ZqZkProps.AssignmentState.NEED_UPDATE);
        ZkUtil.ZkLock.setLockData(zkClient, path, data);
    }

    private void deleteAssignment(CuratorFramework zkClient, String path) {
        String name = StrUtil.subStrAfterLastAssignChar(path, '/');
        if (!isTaskInContainerWithTaskName(name)) {
            return;
        }
        closePathChildCache(name);
        removeTaskFromContainerAndStopTask(name);
        LOGGER.info("delete assignment {} success;", name);
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
        byte[] state = JsonUtil.convertObjToJsonByteArr(STATE, ZqZkProps.AssignmentState.RUNNING);
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
                tryRestartAssignment(client, event.getData().getPath(), event.getData().getData());
                break;
            case CHILD_UPDATED:
                //TODO: data in lock node is changed
                responseToLockStateUpdate(client, event.getData().getPath(), event.getData().getData());
                break;
            default: break;
            }
        });
        addPathChildCacheInContainerWithPath(path, cache);
    }

    private void responseToLockStateUpdate(CuratorFramework zkClient, String path, byte[] data) {
        String assignmentPath = StrUtil.subStrBeforeLastAssignChar(path, '/');
        String assignmentName = StrUtil.subStrAfterLastAssignChar(assignmentPath, '/');
        if (!isTaskInContainerWithTaskName(assignmentName)) {
            //no assignment instance in this broker;
            return;
        }
        if (isNeedUpdate(data)) {
            LOGGER.info("assignment {} is updated; we need stop and reread assignment.", assignmentName);
            //1. remove and stop task;
            //2. release lock;
            removeTaskFromContainerAndStopTask(assignmentName);
            ZkUtil.ZkLock.deleteLock(zkClient, assignmentPath);
        }
    }

    private boolean isNeedUpdate(byte[] data) {
        String state = JsonUtil.convertJsonByteArrToAssignObj(data, STATE, String.class);
        return ZqZkProps.AssignmentState.NEED_UPDATE.equals(state);
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

        byte[] state = JsonUtil.convertObjToJsonByteArr(STATE, ZqZkProps.AssignmentState.RUNNING.getBytes());
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
            scheduler = Scheduler.newScheduler(name, assignment, factory);
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

    private boolean isTaskInContainerWithTaskName(String name) {
        return schedulers.get(name) != null;
    }

    private void addTaskInContainerWithTaskName(String name, Scheduler scheduler) {
        schedulers.put(name, scheduler);
    }

    private void removeTaskFromContainerAndStopTask(String name) {
        Scheduler scheduler = schedulers.get(name);
        if (scheduler != null) {
            scheduler.close();
            schedulers.remove(name);
        }
    }

    private boolean isDeleteAssignment(byte[] data) {
        String state = JsonUtil.convertJsonByteArrToAssignObj(data, STATE, String.class);
        return ZqZkProps.AssignmentState.WAIT_FOR_CLOSE.equals(state);
    }

    private void addPathChildCacheInContainerWithPath(String path, PathChildrenCache cache) {
        pathChildCacheMap.put(path, cache);
    }

    private void closePathChildCache(String path) {
        PathChildrenCache cache = pathChildCacheMap.get(path);
        if (null != cache) {
            try {
                cache.close();
            } catch (Exception e) {
                LOGGER.error("close pathChildCache failed; task node path: {}", path, e);
            }
            pathChildCacheMap.remove(path);
        }
    }
}
