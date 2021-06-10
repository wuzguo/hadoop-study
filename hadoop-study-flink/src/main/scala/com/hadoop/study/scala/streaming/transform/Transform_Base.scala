package com.hadoop.study.scala.streaming.transform

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/10 11:28
 */

object Transform_Base {

    def main(args: Array[String]): Unit = {
        // 获取环境变量
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 从文件读取数据
        val fileStream = env.readTextFile("./hadoop-study-datas/flink/core/sensor.txt")

        // 1. map
        val mapStream = fileStream.map(line => line)
        // mapStream.print("map ")

        // 2. flatmap
        val flatStream = fileStream.flatMap(line => line.split(","))
        // flatStream.print("flatmap ")

        // 3. filter
        val filterStream = fileStream.filter(line => line.split(",")(2).trim.toDouble > 32.0)
        filterStream.print("filter ")

        env.execute("Streaming Transform Base")
    }
}
