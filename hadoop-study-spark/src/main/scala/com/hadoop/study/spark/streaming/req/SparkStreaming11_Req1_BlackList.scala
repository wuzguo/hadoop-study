package com.hadoop.study.spark.streaming.req

import com.hadoop.study.spark.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.ResultSet
import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer

object SparkStreaming11_Req1_BlackList {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming11_Req1_BlackList")
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
        val AdsClick = kafkaDataDS.map(
            kafkaData => {
                val data = kafkaData.value()
                val datas = data.split(" ")
                AdsClick(datas(0), datas(1), datas(2), datas(3), datas(4))
            }
        )

        val ds = AdsClick.transform(
            rdd => {
                // TODO 通过JDBC周期性获取黑名单数据
                val blackList = ListBuffer[String]()

                val conn = JDBCUtil.getConnection
                val pstat = conn.prepareStatement("select user_id from black_list")

                val rs: ResultSet = pstat.executeQuery()
                while (rs.next()) {
                    blackList.append(rs.getString(1))
                }

                rs.close()
                pstat.close()
                conn.close()

                // TODO 判断点击用户是否在黑名单中
                val filterRDD = rdd.filter(
                    data => {
                        !blackList.contains(data.user)
                    }
                )

                // TODO 如果用户不在黑名单中，那么进行统计数量（每个采集周期）
                filterRDD.map(
                    data => {
                        val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
                        val day = dateFormat.format(new java.util.Date(data.ts.toLong))
                        val user = data.user
                        val ad = data.ad

                        ((day, user, ad), 1) // (word, count)
                    }
                ).reduceByKey(_ + _)
            }
        )

        ds.foreachRDD(
            rdd => {
                rdd.foreach {
                    case ((day, user, ad), count) =>
                        println(s"${day} ${user} ${ad} ${count}")
                        if (count >= 30) {
                            // TODO 如果统计数量超过点击阈值(30)，那么将用户拉入到黑名单
                            val conn = JDBCUtil.getConnection
                            val pstat = conn.prepareStatement(
                                """
                                  |insert into black_list (user_id) values (?)
                                  |on DUPLICATE KEY
                                  |UPDATE user_id = ?
                                """.stripMargin)
                            pstat.setString(1, user)
                            pstat.setString(2, user)
                            pstat.executeUpdate()
                            pstat.close()
                            conn.close()
                        } else {
                            // TODO 如果没有超过阈值，那么需要将当天的广告点击数量进行更新。
                            val conn = JDBCUtil.getConnection
                            val pstat = conn.prepareStatement(
                                """
                                  | select
                                  |     *
                                  | from user_ad_count
                                  | where dt = ? and user_id = ? and ad_id = ?
                                """.stripMargin)

                            pstat.setString(1, day)
                            pstat.setString(2, user)
                            pstat.setString(3, ad)
                            val rs = pstat.executeQuery()
                            // 查询统计表数据
                            if (rs.next()) {
                                // 如果存在数据，那么更新
                                val pstat1 = conn.prepareStatement(
                                    """
                                      | update user_ad_count
                                      | set count = count + ?
                                      | where dt = ? and user_id = ? and ad_id = ?
                                    """.stripMargin)
                                pstat1.setInt(1, count)
                                pstat1.setString(2, day)
                                pstat1.setString(3, user)
                                pstat1.setString(4, ad)
                                pstat1.executeUpdate()
                                pstat1.close()
                                // TODO 判断更新后的点击数据是否超过阈值，如果超过，那么将用户拉入到黑名单。
                                val pstat2 = conn.prepareStatement(
                                    """
                                      |select
                                      |    *
                                      |from user_ad_count
                                      |where dt = ? and user_id = ? and ad_id = ? and count >= 30
                                    """.stripMargin)
                                pstat2.setString(1, day)
                                pstat2.setString(2, user)
                                pstat2.setString(3, ad)
                                val rs2 = pstat2.executeQuery()
                                if (rs2.next()) {
                                    val pstat3 = conn.prepareStatement(
                                        """
                                          |insert into black_list (user_id) values (?)
                                          |on DUPLICATE KEY
                                          |UPDATE user_id = ?
                                        """.stripMargin)
                                    pstat3.setString(1, user)
                                    pstat3.setString(2, user)
                                    pstat3.executeUpdate()
                                    pstat3.close()
                                }

                                rs2.close()
                                pstat2.close()
                            } else {
                                // 如果不存在数据，那么新增
                                val pstat1 = conn.prepareStatement(
                                    """
                                      | insert into user_ad_count ( dt, user_id, ad_id, count ) values ( ?, ?, ?, ? )
                                    """.stripMargin)

                                pstat1.setString(1, day)
                                pstat1.setString(2, user)
                                pstat1.setString(3, ad)
                                pstat1.setInt(4, count)
                                pstat1.executeUpdate()
                                pstat1.close()
                            }

                            rs.close()
                            pstat.close()
                            conn.close()
                        }
                }
            }
        )

        // 1. 启动采集器
        ssc.start()
        // 2. 等待采集器的关闭
        ssc.awaitTermination()
    }

    // 广告点击数据
    case class AdsClick(ts: String, area: String, city: String, user: String, ad: String)

}
