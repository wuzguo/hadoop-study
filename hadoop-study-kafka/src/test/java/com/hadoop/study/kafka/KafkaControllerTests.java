package com.hadoop.study.kafka;

import com.hadoop.study.kafka.controller.KafkaController;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * <B>说明：</B><BR>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/1 10:39
 */

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {KafkaApplication.class})
public class KafkaControllerTests {

    @Autowired
    private KafkaController kafkaController;

    @Test
    public void testSend() {
        kafkaController.send("Hello, Value。");
    }
}
