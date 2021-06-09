package com.hadoop.study.scala.streaming.source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/9 21:12
 */

object Source_File {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置并行度
        env.setParallelism(1)

        // 从文件读取数据
        val fileStream = env.readTextFile("./hadoop-study-datas/flink/core/sensor.txt")

        // 打印输出
        fileStream.print()

        // 执行
        env.execute("Streaming Source File")
    }
}
