package com.laomei.zhuque.rest;

import com.laomei.zhuque.KafkaProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;


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
    public Object getTasks() {
        return zkService.getTasks();
    }

    @GetMapping(value = "/{taskName}")
    public Object getTask(@PathVariable String taskName) {
        String taskConfig = zkService.getTask(taskName);
        return null;
    }

    @PostMapping(value = "/")
    public Object createTask(@RequestBody String config) {
        return null;
    }

    @DeleteMapping(value = "/{taskName}")
    public Object deleteTask(@PathVariable String taskName) {
        return null;
    }
}
