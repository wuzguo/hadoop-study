package com.hadoop.study.fraud.detect.controllers;

import com.hadoop.study.fraud.detect.model.Greeting;
import com.hadoop.study.fraud.detect.model.HelloMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.util.HtmlUtils;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/13 17:50
 */

@Slf4j
@Controller
public class HelloController {

    @GetMapping("/index")
    public String hello() {
        return "index";
    }

    @MessageMapping("/hello")
    @SendTo("/topic/greetings")
    public Greeting greeting(HelloMessage message) throws Exception {
        log.info("greetings: {}", message);
        Thread.sleep(200);
        return new Greeting(String.format("Hello, %s", HtmlUtils.htmlEscape(message.getName())));
    }
}
