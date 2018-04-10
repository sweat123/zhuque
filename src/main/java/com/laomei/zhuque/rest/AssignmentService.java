package com.laomei.zhuque.rest;

import com.laomei.zhuque.exception.*;
import org.apache.zookeeper.KeeperException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;


/**
 * @author luobo
 **/
@Component
public class AssignmentService {

    private ZqBrokerServer zqServer;

    private ZqBrokerClient zqClient;

    @Autowired
    public AssignmentService(ZqBrokerServer zqServer, ZqBrokerClient zqClient) throws BrokerClientRegistryException {
        this.zqServer = zqServer;
        this.zqClient = zqClient;
        this.zqClient.start();
        this.zqServer.start();
    }

    /**
     * get all tasks from zk
     * @return all tasks
     */
    public List<String> getTasks() {
        return zqServer.getAllAssignments();
    }

    /**
     * get task configuration with name
     * @param taskName task name
     * @return task configuration or empty string
     */
    public String getTask(String taskName)  {
        return zqServer.getAssignment(taskName);
    }

    /**
     * create task, we will create a node in zk;
     * @param taskName task name
     * @param configuration task configuration
     * @return task configuration or empty string
     * @throws NotValidationException the configuration of task is not valid
     */
    public synchronized String createTask(String taskName, String configuration) throws NotValidationException {
        try {
            if (zqServer.postAssignment(taskName, configuration)) {
                return "post assignment success";
            }
        } catch (KeeperException.NodeExistsException e) {
            throw new NotValidationException("assignment is already existed");
        }
        throw new NotValidationException("The configuration of assignment is not correct;");
    }

    public synchronized String deleteTask(String taskName) throws NotFindException {
        try {
            if (zqServer.deleteAssignment(taskName)) {
                return "delete assignment success";
            }
        } catch (KeeperException.NoNodeException e) {
            throw new NotFindException("assignment is not existed");
        }
        return "unknown error";
    }

    public synchronized String updateTask(String taskName, String configuration) {
        try {
            if (zqServer.updateAssignment(taskName, configuration)) {
                return "update assignment success";
            }
        } catch (KeeperException.NoNodeException e) {
            return "assignment is not existed";
        }
        return "unknown error";
    }
}
