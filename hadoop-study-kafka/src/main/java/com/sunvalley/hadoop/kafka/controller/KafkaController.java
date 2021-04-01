package com.sunvalley.hadoop.kafka.controller;

import com.sunvalley.hadoop.kafka.spring.SpringProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * <B>说明：</B><BR>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/1 10:18
 */

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    @Autowired
    private SpringProducer producer;

    @PostMapping("/send")
    public void send(@RequestBody String value) {
        producer.sendMessage("top-events", value);
    }
}
