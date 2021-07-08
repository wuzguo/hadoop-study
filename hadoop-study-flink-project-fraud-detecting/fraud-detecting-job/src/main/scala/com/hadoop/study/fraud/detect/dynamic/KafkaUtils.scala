package com.hadoop.study.fraud.detect.dynamic

import com.hadoop.study.fraud.detect.config.Config
import com.hadoop.study.fraud.detect.config.Parameters.{KAFKA_HOST, KAFKA_PORT, OFFSET}

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
        val kafkaProps = initProperties(config)
        val offset = config.get(OFFSET)
        kafkaProps.setProperty("auto.offset.reset", offset)
        kafkaProps
    }

    def initProducerProperties(params: Config): Properties = initProperties(params)

    private def initProperties(config: Config): Properties = {
        val kafkaProps = new Properties()
        val kafkaHost = config.get(KAFKA_HOST)
        val kafkaPort = config.get(KAFKA_PORT)
        val servers = String.format("%s:%s", kafkaHost, kafkaPort)
        kafkaProps.setProperty("bootstrap.servers", servers)
        kafkaProps
    }
}
