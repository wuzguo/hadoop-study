package com.hadoop.study.spark.streaming.req

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming13_Req3 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming13_Req3")
        val ssc = new StreamingContext(sparkConf, Seconds(5))

        val kafkaParam = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop002:9092",
            ConsumerConfig.GROUP_ID_CONFIG -> "sparkStreaming",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"
        )


        val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Set("topic_streaming"), kafkaParam)
        )
        val adsClick = kafkaDataDS.map(
            kafkaData => {
                val data = kafkaData.value()
                val datas = data.split(" ")
                AdsClick(datas(0),datas(1),datas(2),datas(3),datas(4))
            }
        )

        // 最近一分钟，每10秒计算一次
        // 12:01 => 12:00
        // 12:11 => 12:10
        // 12:19 => 12:10
        // 12:25 => 12:20
        // 12:59 => 12:50

        // 55 => 50, 49 => 40, 32 => 30
        // 55 / 10 * 10 => 50
        // 49 / 10 * 10 => 40
        // 32 / 10 * 10 => 30

        // 这里涉及窗口的计算
        val reduceDS = adsClick.map(
            data => {
                val ts = data.ts.toLong
                val newTS = ts / 10000 * 10000
                ( newTS, 1 )
            }
        ).reduceByKeyAndWindow((x:Int,y:Int)=>{x+y}, Seconds(60), Seconds(10))

        reduceDS.print()


        ssc.start()
        ssc.awaitTermination()
    }

    // 广告点击数据
    case class AdsClick( ts:String, area:String, city:String, user:String, ad:String )
}
