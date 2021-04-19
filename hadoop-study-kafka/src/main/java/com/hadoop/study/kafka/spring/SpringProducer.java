package com.hadoop.study.kafka.spring;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * <B>说明：</B><BR>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/1 10:11
 */

@Component
public class SpringProducer {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    public void sendMessage(String topicName, Object message) {
        kafkaTemplate.send(topicName, message).addCallback(result -> System.out.println("发送成功"), ex -> System.out.println(ex.getMessage()));
    }
}
