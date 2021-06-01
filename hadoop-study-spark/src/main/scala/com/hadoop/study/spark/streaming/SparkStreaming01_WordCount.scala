package com.hadoop.study.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/1 13:49
 */

object SparkStreaming01_WordCount {

    def main(args: Array[String]): Unit = {
        // StreamingContext创建时，需要传递两个参数
        // 第一个参数表示环境配置
        val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")
        // 第二个参数表示批量处理的周期（采集周期）
        val ssc = new StreamingContext(conf, Seconds(3))

        // 获取端口数据
        val lines = ssc.socketTextStream("hadoop002", 9999)
        // 切分字符串
        val words = lines.flatMap(_.split(" "))

        val wordToOne = words.map((_, 1))

        val wordToCount = wordToOne.reduceByKey(_ + _)
        // 打印
        wordToCount.print()

        // 由于SparkStreaming采集器是长期执行的任务，所以不能直接关闭
        // 如果main方法执行完毕，应用程序也会自动结束。所以不能让main执行完毕
        //ssc.stop()
        // 1. 启动采集器
        ssc.start()
        // 2. 等待采集器的关闭
        ssc.awaitTermination()
    }
}
