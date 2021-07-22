package com.hadoop.study.spark.recomnend.producer

import org.apache.kafka.streams.processor.{Processor, ProcessorSupplier, TopologyBuilder}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import java.util.Properties

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/22 10:07
 */

object KafkaProducer {

    def main(args: Array[String]): Unit = {
        // 定义拓扑构建器
        val builder = new TopologyBuilder
        builder.addSource("SOURCE", "recommender-log")
          .addProcessor("PROCESSOR",
              new ProcessorSupplier[String, String] {
                  override def get(): Processor[String, String] = LogProcessor()
              }, "SOURCE")
          .addSink("SINK", "recommender", "PROCESSOR")

        // 创建kafka stream
        val streams = new KafkaStreams(builder, initConfig("10.20.0.92", 9092))
        streams.start()
    }


    def initConfig(host: String, port: Int): StreamsConfig = {
        val properties = new Properties()
        val servers = String.format("%s:%s", host, port)
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, servers)
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter")
        new StreamsConfig(properties)
    }
}
