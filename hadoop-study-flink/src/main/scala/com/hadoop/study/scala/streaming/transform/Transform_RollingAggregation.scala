package com.hadoop.study.scala.streaming.transform

import com.hadoop.study.scala.streaming.beans.Sensor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/10 11:38
 */

object Transform_RollingAggregation {

    def main(args: Array[String]): Unit = {
        // 环境变量
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置并行度
        env.setParallelism(4)

        // 从文件读取数据
        val fileStream = env.readTextFile("./hadoop-study-datas/flink/core/sensor.txt")

        // 构造Stream
        val sensorStream = fileStream.map(line => {
            val values = line.split(",")
            Sensor(values(0), values(1).trim.toLong, values(2).trim.toDouble)
        })

        // 分组
        val keyStream = sensorStream.keyBy(_.id)
        keyStream.print("key ")
        // 滚动聚合，取当前最大的温度值
        val maxStream = keyStream.maxBy("temp")
        maxStream.print("max ")

        //
        val dataStream = env.fromElements(1, 34, 4, 657, 23)
        val keyStream2 = dataStream.keyBy(_ % 2)

        // 打印
        keyStream2.sum(0).print("key2 ")

        env.execute("Streaming Transform RollingAggregation")
    }
}
