package com.hadoop.study.scala.streaming.sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.kafka.clients.consumer.ConsumerConfig

import java.util.Properties

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/9 21:45
 */

object Sink_Kafka {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置并行度
        env.setParallelism(1)

        val properties = new Properties
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadop002:9092")
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "sparkStreaming")
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

        // 读取数据
        val inputStream = env.addSource(new FlinkKafkaConsumer("topic_streaming", new SimpleStringSchema, properties))
        inputStream.print("kafka ")

        val sinkValues = inputStream.map(line => {
            line.split(" ").mkString(", ")
        })
        // 发送到Kafka
        sinkValues.addSink(new FlinkKafkaProducer("topic_streaming_sink", new SimpleStringSchema, properties))

        // 执行
        env.execute("Streaming Sink Kafka")
    }
}
