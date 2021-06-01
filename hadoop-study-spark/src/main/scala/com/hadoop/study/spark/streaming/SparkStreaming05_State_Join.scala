package com.hadoop.study.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/1 15:12
 */

object SparkStreaming05_State_Join {

    def main(args: Array[String]): Unit = {
        // StreamingContext创建时，需要传递两个参数
        // 第一个参数表示环境配置
        val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming05_State_Join")

        val ssc = new StreamingContext(conf, Seconds(5))

        val data9999 = ssc.socketTextStream("hadoop002", 9999)
        val data8888 = ssc.socketTextStream("hadoop002", 8888)

        val map9999: DStream[(String, Int)] = data9999.map((_, 9))
        val map8888: DStream[(String, Int)] = data8888.map((_, 8))

        // 所谓的DStream的Join操作，其实就是两个RDD的join
        val joinDS: DStream[(String, (Int, Int))] = map9999.join(map8888)

        // 打印
        joinDS.print()

        // 1. 启动采集器
        ssc.start()
        // 2. 等待采集器的关闭
        ssc.awaitTermination()
    }
}
