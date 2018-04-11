package com.laomei.zhuque.rest.page;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * @author luobo
 **/
@Controller
public class PageController {

    @GetMapping(value = "/index")
    public String index() {
        return home();
    }

    @GetMapping(value = "")
    public String home() {
        return "index";
    }

    @GetMapping(value = "/new")
    public String newAssignment() {
        return "newAssignment";
    }

    @GetMapping(value = "/all")
    public String allAssignment() {
        return "allAssignment";
    }

    @GetMapping(value = "/display")
    public String displayAssignment() {
        return "displayAssignment";
    }

    @GetMapping(value = "/delete")
    public String deleteAssignment() {
        return "deleteAssignment";
    }
}
