package com.laomei.zhuque.rest;

import com.laomei.zhuque.config.ZqInstanceFactory;
import com.laomei.zhuque.core.Scheduler;
import com.laomei.zhuque.core.SyncAssignment;
import com.laomei.zhuque.exception.*;
import com.laomei.zhuque.util.ZhuQueZkPathEnum;
import com.laomei.zhuque.util.ZkUtil;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

/**
 * @author luobo
 **/
@Component
public class AssignmentService {

    private CuratorFramework zkCli;

    private ExecutorService threadPool;

    private Set<String> taskNameStore;

    private Map<String, Scheduler> schedulersStore;

    private Map<String, SyncAssignment> assignmentStore;

    private Map<String, Future> futureStore;

    @Autowired
    private ZqInstanceFactory factory;

    @Autowired
    public AssignmentService(ZqInstanceFactory factory) {
        zkCli = factory.zkClient();
        taskNameStore = new HashSet<>();
        schedulersStore = new HashMap<>();
        futureStore = new HashMap<>();
        assignmentStore = new HashMap<>();
        threadPool = Executors.newCachedThreadPool(r -> new Thread(r, "ZhuQue-assignment-thread"));
    }

    /**
     * get all tasks from zk
     * @return all tasks
     */
    public List<String> getTasks() {
        String taskRootPath = ZhuQueZkPathEnum.TASK_ROOT_PATH.getPath();
        return ZkUtil.getAllChildren(zkCli, taskRootPath);
    }

    /**
     * get task configuration with name
     * @param taskName task name
     * @return task configuration or empty string
     * @throws NotFindException can't find task
     */
    public String getTask(String taskName) throws NotFindException {
        String taskPath = ZhuQueZkPathEnum.TASK_ROOT_PATH + "/" + taskName;
        byte[] task = ZkUtil.getNodeData(zkCli, taskPath);
        if (task == null) {
            throw new NotFindException("can't not find sync assignment: " + taskName);
        }
        return new String(task);
    }

    /**
     * create task, we will create a node in zk;
     * @param taskName task name
     * @param configuration task configuration
     * @return task configuration or empty string
     * @throws NotValidationException the configuration of task is not valid
     */
    public synchronized String createTask(String taskName, String configuration) throws NotValidationException {
        if (taskNameStore.contains(taskName)) {
            return "the name of assignment is already exist";
        }
        AssignmentValidator validator = AssignmentValidator.getValidator();
        if (validator.isValid(configuration)) {
            SyncAssignment assignment = SyncAssignment.newSyncTaskMetadata(configuration);
            try {
                Scheduler scheduler = Scheduler.newScheduler(assignment, factory);
                taskNameStore.add(taskName);
                schedulersStore.put(taskName, scheduler);
                assignmentStore.put(taskName, assignment);
                Future future = threadPool.submit(scheduler::start);
                futureStore.put(taskName, future);
            } catch (UnknownReducerClazzException e) {
                releaseResources(taskName);
                removeTaskNameFromStore(taskName);
                return "the reducer clazz in configuration is not correct";
            } catch (NullReducerClazzException e) {
                releaseResources(taskName);
                removeTaskNameFromStore(taskName);
                return "the reducer clazz can't be empty";
            } catch (InitSchemaFailedException e) {
                releaseResources(taskName);
                removeTaskNameFromStore(taskName);
                return "init schemas failed; " + e;
            } catch (Exception e) {
                releaseResources(taskName);
                removeTaskNameFromStore(taskName);
                return "unknown error; " + e;
            }
            return configuration;
        }
        return "The configuration of assignment is not correct;";
    }

    public synchronized String deleteTask(String taskName) throws NotFindException {
        String taskPath = ZhuQueZkPathEnum.TASK_ROOT_PATH + "/" + taskName;
        if (!ZkUtil.ensurePath(zkCli, taskPath)) {
            throw new NotFindException("sync assignment not exist; please check your input name;");
        }
        //here we need delete task node; but firstly we should set task state deleted;
        return null;
    }

    public synchronized void clear() {
        for (String name : taskNameStore) {
            releaseResources(name);
        }
        taskNameStore.clear();
        taskNameStore = null;
        schedulersStore = null;
        futureStore = null;
        assignmentStore = null;
    }

    private void releaseResources(String taskName) {
        removeAssignmentFromStore(taskName);
        Scheduler scheduler = schedulersStore.get(taskName);
        removeSchedulerFromStore(taskName);
        scheduler.close();
        Future future = futureStore.get(taskName);
        removeFutureFromStore(taskName);
        while (future != null && !future.isDone()) {
            LockSupport.parkNanos(1000000);
        }
    }

    private void removeTaskNameFromStore(String taskName) {
        taskNameStore.remove(taskName);
    }

    private void removeSchedulerFromStore(String taskName) {
        schedulersStore.remove(taskName);
    }

    private void removeAssignmentFromStore(String taskName) {
        assignmentStore.remove(taskName);
    }

    private void removeFutureFromStore(String taskName) {
        futureStore.remove(taskName);
    }
}
