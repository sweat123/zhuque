package com.laomei.zhuque.rest;

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

    @Autowired
    private CuratorFramework zkCli;

    public List<String> getTasks() {
        String taskRootPath = ZhuQueZkPathEnum.TASK_ROOT_PATH.getPath();
        List<String> tasks = ZkUtil.getAllChildren(zkCli, taskRootPath);
        return tasks;
    }

    public String getTask(String taskName) {
        String taskPath = ZhuQueZkPathEnum.TASK_ROOT_PATH + "/" + taskName;
        byte[] task = ZkUtil.getNodeData(zkCli, taskPath);
        return task == null ? StringUtil.EMPTY_STR : new String(task);
    }
}
