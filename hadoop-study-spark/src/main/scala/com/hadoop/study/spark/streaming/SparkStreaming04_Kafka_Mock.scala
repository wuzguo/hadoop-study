package com.hadoop.study.spark.streaming

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.{Properties, Random}
import scala.collection.mutable.ListBuffer

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/1 14:38
 */

object SparkStreaming04_Kafka_Mock {

    def main(args: Array[String]): Unit = {

        // 生成模拟数据
        // 格式 ：timestamp area city userId adId
        // 含义： 时间戳  区域  城市 用户 广告
        // Application => Kafka => SparkStreaming => Analysis
        val properties = new Properties()
        // 添加配置
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadop002:9092")
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

        // 生产者
        val producer = new KafkaProducer[String, String](properties)

        while (true) {
            mock().foreach(
                data => {
                    // 向Kafka中生成数据
                    val record = new ProducerRecord[String, String]("topic_streaming", data)
                    producer.send(record)
                    println(data)
                }
            )

            Thread.sleep(2000)
        }

    }

    /**
     * MOCK方法
     *
     * @return {@link ListBuffer}
     */
    def mock(): ListBuffer[String] = {
        val list = ListBuffer[String]()
        val areaList = ListBuffer[String]("华北", "华东", "华南")
        val cityList = ListBuffer[String]("北京", "上海", "深圳")

        for (i <- 1 to new Random().nextInt(50)) {
            val area = areaList(new Random().nextInt(3))
            val city = cityList(new Random().nextInt(3))
            val userId = new Random().nextInt(6) + 1
            val adId = new Random().nextInt(6) + 1
            list.append(s"${System.currentTimeMillis()} ${area} ${city} ${userId} ${adId}")
        }
        list
    }
}
