package com.sunvalley.hadoop.kafka.constom;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * <B>说明：</B><BR>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/1 9:48
 */

public class CustomProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.50.2.220:9092");
        props.put("acks", "all");
        // 重试次数
        props.put("retries", 1);
        // 批次大小
        props.put("batch.size", 16384);
        // 等待时间
        props.put("linger.ms", 1);
        // RecordAccumulator缓冲区大小
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 2 构建拦截链
        List<String> interceptors = Lists.newArrayList();
        interceptors.add("com.sunvalley.hadoop.kafka.interceptor.TimeInterceptor");
        interceptors.add("com.sunvalley.hadoop.kafka.interceptor.CounterInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
        // 发送消息
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 10; i++) {
                //回调函数，该方法会在Producer收到ack时调用，为异步调用
                ProducerRecord record = new ProducerRecord<String, String>("top-events", 0, Integer.toString(i),
                    Integer.toString(i));
                producer.send(record).get();
            }
        }
    }
}
