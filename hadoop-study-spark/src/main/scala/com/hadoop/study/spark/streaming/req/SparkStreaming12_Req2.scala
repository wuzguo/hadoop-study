package com.hadoop.study.spark.streaming.req

import java.text.SimpleDateFormat
import com.hadoop.study.spark.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming12_Req2 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming12_Req2")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

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

        val reduceDS = adsClick.map(
            data => {
                val sdf = new SimpleDateFormat("yyyy-MM-dd")
                val day = sdf.format(new java.util.Date( data.ts.toLong ))
                val area = data.area
                val city = data.city
                val ad = data.ad

                ( ( day, area, city, ad ), 1 )
            }
        ).reduceByKey(_+_)

        reduceDS.foreachRDD(
            rdd => {
                rdd.foreachPartition(
                    iter => {
                        val conn = JDBCUtil.getConnection
                        val pstat = conn.prepareStatement(
                            """
                              | insert into area_city_ad_count ( dt, area, city, ad_id, count )
                              | values ( ?, ?, ?, ?, ? )
                              | on DUPLICATE KEY
                              | UPDATE count = count + ?
                            """.stripMargin)
                        iter.foreach{
                            case ( ( day, area, city, ad ), sum ) =>
                                pstat.setString(1,day )
                                pstat.setString(2,area )
                                pstat.setString(3, city)
                                pstat.setString(4, ad)
                                pstat.setInt(5, sum)
                                pstat.setInt(6,sum )
                                pstat.executeUpdate()
                        }
                        pstat.close()
                        conn.close()
                    }
                )
            }
        )

        // 1. 启动采集器
        ssc.start()
        // 2. 等待采集器的关闭
        ssc.awaitTermination()
    }

    // 广告点击数据
    case class AdsClick( ts:String, area:String, city:String, user:String, ad:String )
}
