package com.hadoop.study.scala.streaming.wc

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/7 10:49
 */

object WordCount {

    def main(args: Array[String]): Unit = {
        // 1. 获取环境配置
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        // 2. 从Socket读取文件
        val dss: DataStream[String] = env.readTextFile("./hadoop-study-datas/flink/core/1.txt")

        val windowCounts = dss.flatMap { line => line.split(" ") }
          .filter(_.nonEmpty)
          .map { word => WordWithCount(word, 1) }
          .keyBy(_.word)
          // 滚动窗口2秒
          // .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
          .sum("count")

        // print the results with a single thread, rather than in parallel
        windowCounts.print("count ")

        env.execute("Streaming WordCount")
    }

    /** Data type for words with count */
    case class WordWithCount(word: String, count: Long)
}
