package com.hadoop.study.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/1 13:49
 */

object SparkStreaming02_Queue {

    def main(args: Array[String]): Unit = {
        // StreamingContext创建时，需要传递两个参数
        // 第一个参数表示环境配置
        val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming02_Queue")
        // 第二个参数表示批量处理的周期（采集周期）
        val ssc = new StreamingContext(conf, Seconds(3))

        val rddQueue = new mutable.Queue[RDD[Int]]()

        val inputStream = ssc.queueStream(rddQueue, oneAtATime = false)
        val mappedStream = inputStream.map((_, 1))
        val reducedStream = mappedStream.reduceByKey(_ + _)
        // 打印
        reducedStream.print()

        // 1. 启动采集器
        ssc.start()

        for (_ <- 1 to 5) {
            rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
            Thread.sleep(2000)
        }
        // 2. 等待采集器的关闭
        ssc.awaitTermination()
    }
}
