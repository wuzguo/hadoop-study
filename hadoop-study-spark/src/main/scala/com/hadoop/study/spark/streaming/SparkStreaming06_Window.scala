package com.hadoop.study.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_Window {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming06_Window")
        val ssc = new StreamingContext(conf, Seconds(3))

        val lines = ssc.socketTextStream("hadoop002", 9999)
        val wordToOne = lines.map((_,1))

        // 窗口的范围应该是采集周期的整数倍
        // 窗口可以滑动的，但是默认情况下，一个采集周期进行滑动
        // 这样的话，可能会出现重复数据的计算，为了避免这种情况，可以改变滑动的滑动（步长）
        val windowDS: DStream[(String, Int)] = wordToOne.window(Seconds(6), Seconds(6))

        val wordToCount = windowDS.reduceByKey(_+_)

        wordToCount.print()

        // 1. 启动采集器
        ssc.start()
        // 2. 等待采集器的关闭
        ssc.awaitTermination()
    }
}
