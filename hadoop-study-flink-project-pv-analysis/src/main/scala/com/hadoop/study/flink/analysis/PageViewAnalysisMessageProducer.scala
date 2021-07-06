package com.hadoop.study.flink.analysis

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.{Properties, Random}
import scala.collection.mutable.ListBuffer

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/6 13:42
 */

object PageViewAnalysisMessageProducer {

    def main(args: Array[String]): Unit = {
        // 生成模拟数据
        // 格式 ：timestamp area city userId adId
        // 含义： 时间戳  区域  城市 用户 广告
        // Application => Kafka => SparkStreaming => Analysis
        val properties = new Properties()
        // 添加配置
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.20.0.92:9092")
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

        // 生产者
        val producer = new KafkaProducer[String, String](properties)
        while (true) {
            // 向Kafka中生成数据
            mockBehavior(1).foreach(behavior => {
                val record = new ProducerRecord[String, String]("topic_behavior", behavior)
                producer.send(record)
                println(behavior)
                // 随机暂停
                Thread.sleep(new Random().nextInt(100))
            })
        }
    }

    /**
     * MOCK方法
     *
     * @param num 生产的数据个数
     * @return {ListBuffer}
     */
    def mockBehavior(num: Int): ListBuffer[String] = {
        val behaviors = ListBuffer[String]()
        for (_ <- 1 to num) {
            val actions = ListBuffer[String]("pv", "fav", "pv", "buy", "pv", "cart", "pv")
            val action = actions(new Random().nextInt(7))
            val random = new Random()
            val userId = random.nextInt(10000)
            val itemId = random.nextInt(10000)
            val categoryId = random.nextInt(1000)
            behaviors.append(s"${userId},${itemId},${categoryId},${action},${System.currentTimeMillis()}")
        }
        behaviors
    }
}
