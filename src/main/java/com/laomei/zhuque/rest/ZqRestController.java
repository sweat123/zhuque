package com.laomei.zhuque.rest;

import com.laomei.zhuque.core.SyncAssignment;
import com.laomei.zhuque.exception.NotFindException;
import com.laomei.zhuque.exception.NotValidationException;
import com.laomei.zhuque.rest.rspdata.Result;
import com.laomei.zhuque.rest.rspdata.SyncAssignmentVo;
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
    private AssignmentService assignmentService;

    @GetMapping(value = "/")
    public Result<List<String>> getTasks() {
        return Result.ok(assignmentService.getTasks());
    }

    @GetMapping(value = "/{taskName}")
    public Result<?> getTask(@PathVariable String taskName) {
        String config = assignmentService.getTask(taskName);
        return Result.ok(new SyncAssignmentVo(taskName, SyncAssignment.newSyncTaskMetadata(config)));
    }

    @PostMapping(value = "/{taskName}")
    public Result<?> createTask(@PathVariable String taskName, @RequestBody String config) {
        try {
            assignmentService.createTask(taskName, config);
            return Result.ok(SyncAssignment.newSyncTaskMetadata(config));
        } catch (NotValidationException e) {
            return Result.badRequest("message: " + e);
        }
    }

    @DeleteMapping(value = "/{taskName}")
    public Result<?> deleteTask(@PathVariable String taskName) {
        try {
            return Result.ok(assignmentService.deleteTask(taskName));
        } catch (NotFindException e) {
            return Result.notFount("message: " + e);
        }
    }

    @PutMapping(value = "/{taskName}")
    public Result<String> updateTask(@PathVariable String taskName, @RequestBody String config) {
        return Result.ok(assignmentService.updateTask(taskName, config));
    }
}
