package com.hadoop.study.scala.streaming.transform

import com.hadoop.study.scala.streaming.beans.Sensor
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/10 11:38
 */

object Transform_Reduce {

    def main(args: Array[String]): Unit = {
        // 环境变量
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置并行度
        env.setParallelism(1)

        // 从文件读取数据
        val fileStream = env.readTextFile("./hadoop-study-datas/flink/core/sensor.txt")

        // 构造Stream
        val sensorStream = fileStream.map(line => {
            val values = line.split(",")
            Sensor(values(0), values(1).trim.toLong, values(2).trim.toDouble)
        })

        // 分组
        val keyStream = sensorStream.keyBy(_.id)
        // 取最大的
        val reduceStream = keyStream.reduce(new ReduceFunction[Sensor] {

            override def reduce(value1: Sensor, value2: Sensor): Sensor = {
                Sensor(value1.id, value2.timestamp, Math.max(value1.temp, value2.temp))
            }
        })
        reduceStream.print("reduce ")

        val reduceStream2 = keyStream.reduce((sensor, _) => sensor)
        reduceStream2.print("reduce2 ")

        env.execute("Streaming Transform Reduce")
    }
}
