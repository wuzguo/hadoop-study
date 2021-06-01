package com.hadoop.study.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object SparkStreaming08_Close {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming08_Close")

        val ssc = new StreamingContext(conf, Seconds(3))

        val lines = ssc.socketTextStream("hadoop002", 9999)
        val wordToOne = lines.map((_, 1))
        wordToOne.print()

        ssc.start()

        // 如果想要关闭采集器，那么需要创建新的线程
        // 而且需要在第三方程序中增加关闭状态
        new Thread(() => {
            // 优雅地关闭
            // 计算节点不在接收新的数据，而是将现有的数据处理完毕，然后关闭
            // Mysql : Table(stopSpark) => Row => data
            // Redis : Data（K-V）
            // ZK    : /stopSpark
            // HDFS  : /stopSpark
            /*
            while ( true ) {
                if (true) {
                    // 获取SparkStreaming状态
                    val state: StreamingContextState = ssc.getState()
                    if ( state == StreamingContextState.ACTIVE ) {
                        ssc.stop(true, true)
                    }
                }
                Thread.sleep(5000)
            }
             */

            Thread.sleep(5000)
            val state: StreamingContextState = ssc.getState()
            if (state == StreamingContextState.ACTIVE) {
                ssc.stop(stopSparkContext = true, stopGracefully = true)
            }
            System.exit(0)
        }
        ).start()

        // block 阻塞main线程
        ssc.awaitTermination()
    }
}
