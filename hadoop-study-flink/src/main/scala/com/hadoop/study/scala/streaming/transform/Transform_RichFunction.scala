package com.hadoop.study.scala.streaming.transform

import com.hadoop.study.scala.streaming.beans.Sensor
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/10 11:38
 */

object Transform_RichFunction {

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

        val richStream = sensorStream.map(new CustomRichFunction)
        richStream.print("rich ")

        env.execute("Streaming Transform RichFunction")
    }

    // 自定义函数
    class CustomRichFunction extends RichMapFunction[Sensor, (String, Double)] {
        override def map(value: Sensor): (String, Double) = (value.id, value.temp)

        override def open(parameters: Configuration): Unit = {
            println(s"open: $parameters")
            super.open(parameters)
        }

        override def close(): Unit = {
            println("close")
            super.close()
        }
    }
}
