package com.hadoop.study.recommend.util;

import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class CustomKafkaProducer {

    private static final String RATING_TOPIC = "topic-recommend-rating";

    private static final KafkaProducer producer;

    static {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Property.getStrValue("kafka.bootstrap.servers"));
        producer = new KafkaProducer(properties);
    }

    public static void produce(String msg) {
        log.info("produce: {}", msg);
        ProducerRecord<String, String> record = new ProducerRecord<>(RATING_TOPIC, msg);
        producer.send(record);
    }
}
