package com.hadoop.study.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreaming09_Resume {

    def main(args: Array[String]): Unit = {
        // 获取检查点的数据
        val ssc = StreamingContext.getActiveOrCreate("hdfs://hadoop001:9000/user/spark/streaming/cp",
            () => {
                val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming09_Resume")
                val ssc = new StreamingContext(conf, Seconds(3))

                val lines = ssc.socketTextStream("hadoop002", 9999)
                val wordToOne = lines.map((_, 1))

                wordToOne.print()
                ssc
            })
        // 设置检查点
        ssc.checkpoint("hdfs://hadoop001:9000/user/spark/streaming/cp")
        // 1. 启动采集器
        ssc.start()
        // block 阻塞main线程
        ssc.awaitTermination()
    }

}
