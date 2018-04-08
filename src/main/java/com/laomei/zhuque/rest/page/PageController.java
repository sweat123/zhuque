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
}
