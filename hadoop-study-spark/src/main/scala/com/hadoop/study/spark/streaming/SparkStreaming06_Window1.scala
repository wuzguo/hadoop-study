package com.hadoop.study.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_Window1 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming06_Window1")
        val ssc = new StreamingContext(conf, Seconds(3))
        // 设置检查点
        ssc.checkpoint("hdfs://hadoop001:9000/user/spark/streaming/cp")

        val lines = ssc.socketTextStream("hadoop002", 9999)
        val wordToOne = lines.map((_,1))

        // reduceByKeyAndWindow : 当窗口范围比较大，但是滑动幅度比较小，那么可以采用增加数据和删除数据的方式
        // 无需重复计算，提升性能。
        val windowDS: DStream[(String, Int)] =
            wordToOne.reduceByKeyAndWindow(
                (x:Int, y:Int) => { x + y},
                (x:Int, y:Int) => {x - y},
                Seconds(9), Seconds(3))

        windowDS.print()

        // 1. 启动采集器
        ssc.start()
        // 2. 等待采集器的关闭
        ssc.awaitTermination()
    }
}
