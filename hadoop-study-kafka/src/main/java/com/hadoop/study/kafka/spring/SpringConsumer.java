package com.hadoop.study.kafka.spring;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

/**
 * <B>说明：</B><BR>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/1 10:30
 */

@Component
public class SpringConsumer {

    @KafkaListener(topics = "top-events", topicPartitions = {
        @TopicPartition(topic = "top-events", partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "-1"))})
    public void topEvents(Object value) {
        System.out.println(value);
    }
}
