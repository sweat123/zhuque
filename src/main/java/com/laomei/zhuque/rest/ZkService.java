package com.laomei.zhuque.rest;

import com.laomei.zhuque.ZqInstanceFactory;
import com.laomei.zhuque.core.SyncAssignment;
import com.laomei.zhuque.exception.NotFindException;
import com.laomei.zhuque.exception.NotValidationException;
import com.laomei.zhuque.util.StringUtil;
import com.laomei.zhuque.util.ZhuQueZkPathEnum;
import com.laomei.zhuque.util.ZkUtil;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author luobo
 **/
@Component
public class ZkService {

    private CuratorFramework zkCli;

    @Autowired
    public ZkService(ZqInstanceFactory factory) {
        zkCli = factory.zkClient();
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
    public String createTask(String taskName, String configuration) throws NotValidationException {
        AssignmentValidator validator = AssignmentValidator.getValidator();
        if (validator.isValid(configuration)) {
            SyncAssignment assignment = SyncAssignment.newSyncTaskMetadata(configuration);
            //create task with configuration
            return configuration;
        }
        return StringUtil.EMPTY_STR;
    }

    public String deleteTask(String taskName) throws NotFindException {
        String taskPath = ZhuQueZkPathEnum.TASK_ROOT_PATH + "/" + taskName;
        if (!ZkUtil.ensurePath(zkCli, taskPath)) {
            throw new NotFindException("sync assignment not exist; please check your input name;");
        }
        //here we need delete task node; but firstly we should set task state deleted;
        return null;
    }
}
