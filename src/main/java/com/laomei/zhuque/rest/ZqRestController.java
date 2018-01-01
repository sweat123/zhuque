package com.laomei.zhuque.rest;

import com.google.common.base.Objects;
import com.laomei.zhuque.KafkaProperties;
import com.laomei.zhuque.core.SyncAssignment;
import com.laomei.zhuque.exception.NotFindException;
import com.laomei.zhuque.exception.NotValidationException;
import com.laomei.zhuque.rest.rspdata.Result;
import com.laomei.zhuque.rest.rspdata.SyncAssignmentVo;
import com.laomei.zhuque.util.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * @author luobo
 **/
@RestController
@RequestMapping(value = "/api/zhuque")
public class ZqRestController {

    @Autowired
    private ZkService zkService;

    @Autowired
    private KafkaProperties kafkaProperties;

    @GetMapping(value = "/")
    public Result<List<String>> getTasks() {
        return Result.ok(zkService.getTasks());
    }

    @GetMapping(value = "/{taskName}")
    public Result<?> getTask(@PathVariable String taskName) {
        try {
            String config = zkService.getTask(taskName);
            return Result.ok(new SyncAssignmentVo(taskName, SyncAssignment.newSyncTaskMetadata(config)));
        } catch (NotFindException e) {
            return Result.notFount("message: " + e.getMessage());
        }
    }

    @PostMapping(value = "/{taskName}")
    public Result<?> createTask(@PathVariable String taskName, @RequestBody String config) {
        try {
            zkService.createTask(taskName, config);
        } catch (NotValidationException e) {
            return Result.badRequest("message: " + e.getMessage() + "\ncause: " + e.getCause());
        }
        return Result.ok(SyncAssignment.newSyncTaskMetadata(config));
    }

    @DeleteMapping(value = "/{taskName}")
    public Object deleteTask(@PathVariable String taskName) {
        try {
            zkService.deleteTask(taskName);
        } catch (NotFindException e) {
            return Result.notFount("message: " + e.getMessage());
        }
        return Result.ok(StringUtil.EMPTY_STR);
    }
}
