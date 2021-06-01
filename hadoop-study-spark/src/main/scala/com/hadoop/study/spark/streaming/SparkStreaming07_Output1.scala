package com.hadoop.study.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming07_Output1 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming07_Output1")
        val ssc = new StreamingContext(conf, Seconds(3))
        // 设置检查点
        ssc.checkpoint("hdfs://hadoop001:9000/user/spark/streaming/cp")

        val lines = ssc.socketTextStream("hadoop002", 9999)
        val wordToOne = lines.map((_,1))
        
        val windowDS: DStream[(String, Int)] =
            wordToOne.reduceByKeyAndWindow(
                (x:Int, y:Int) => { x + y},
                (x:Int, y:Int) => {x - y},
                Seconds(9), Seconds(3))

        // foreachRDD不会出现时间戳
        windowDS.foreachRDD(
            rdd => {

            }
        )
        // 1. 启动采集器
        ssc.start()
        // 2. 等待采集器的关闭
        ssc.awaitTermination()
    }
}
