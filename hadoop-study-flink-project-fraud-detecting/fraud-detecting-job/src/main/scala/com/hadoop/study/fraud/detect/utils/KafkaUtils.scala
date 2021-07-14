package com.hadoop.study.fraud.detect.utils

import com.hadoop.study.fraud.detect.config.Config
import com.hadoop.study.fraud.detect.config.Parameters.{KAFKA_HOST, KAFKA_PORT, OFFSET}
import org.apache.kafka.clients.consumer.ConsumerConfig

import java.util.Properties

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 15:08
 */

object KafkaUtils {

    def initConsumerProperties(config: Config): Properties = {
        val properties = initProperties(config)
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.get(OFFSET))
        properties
    }

    def initProducerProperties(config: Config): Properties = initProperties(config)

    private def initProperties(config: Config): Properties = {
        val properties = new Properties()
        val kafkaHost = config.get(KAFKA_HOST)
        val kafkaPort = config.get(KAFKA_PORT)
        val servers = String.format("%s:%s", kafkaHost, kafkaPort)
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers)
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "fraud-detect-group")
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
        properties
    }
}
