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

object SparkStreaming06_Transform {

    def main(args: Array[String]): Unit = {
        // StreamingContext创建时，需要传递两个参数
        // 第一个参数表示环境配置
        val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming06_Transform")

        val ssc = new StreamingContext(conf, Seconds(3))

        val lines = ssc.socketTextStream("hadoop002", 9999)

        // transform方法可以将底层RDD获取到后进行操作
        // 1. DStream功能不完善
        // 2. 需要代码周期性的执行

        // Code : Driver端
        val newDS: DStream[String] = lines.transform(
            rdd => {
                // Code : Driver端，（周期性执行）
                rdd.map(
                    str => {
                        // Code : Executor端
                        str
                    }
                )
            }
        )
        // Code : Driver端
        val newDS1: DStream[String] = lines.map(
            data => {
                // Code : Executor端
                data
            }
        )
        // 打印
        newDS.print()

        // 1. 启动采集器
        ssc.start()
        // 2. 等待采集器的关闭
        ssc.awaitTermination()
    }
}
