package com.hadoop.study.fraud.detect.controllers;

import org.springframework.stereotype.Controller;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/13 17:50
 */

@Controller
public class HelloController {

    @MessageMapping("/hello")
    public Greeting greeting(HelloMessage message) throws Exception {
        Thread.sleep(1000); // simulated delay
        return new Greeting("Hello, " + HtmlUtils.htmlEscape(message.getName()) + "!");
    }

}
