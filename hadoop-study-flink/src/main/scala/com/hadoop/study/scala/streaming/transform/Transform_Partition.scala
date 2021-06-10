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

object Transform_Partition {

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
        // sensorStream.print("sensor ")

        // 1. shuffle
        val shuffleStream = fileStream.shuffle
        // shuffleStream.print("shuffle ")

        // 2. keyBy
        // sensorStream.keyBy(_.id).print("keyBy")

        // 3. global
        sensorStream.global.print("global")

        env.execute("Streaming Transform RichFunction")
    }
}
