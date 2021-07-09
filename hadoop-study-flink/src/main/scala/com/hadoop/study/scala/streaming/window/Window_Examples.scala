package com.hadoop.study.scala.streaming.window

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{KeyedStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration


/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/9 14:44
 */

object Window_Examples {

    def main(args: Array[String]): Unit = {
        // 环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        val dstream = env.socketTextStream("hadoop003", 9999)

        val textWithTsStream = dstream.map { text =>
            val arr: Array[String] = text.split(" ")
            (arr(0), arr(1).toLong, 1)
        }

        val textWithEventTimeStream = textWithTsStream.assignTimestampsAndWatermarks(
            WatermarkStrategy.forBoundedOutOfOrderness[(String, Long, Int)](Duration.ofSeconds(1))
              .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long, Int)] {
                  override def extractTimestamp(element: (String, Long, Int), recordTimestamp: Long): Long = element._2
              }))

        val textKeyStream: KeyedStream[(String, Long, Int), Tuple] = textWithEventTimeStream.keyBy(0)

        textKeyStream.print("text key: ")

        val windowStream = textKeyStream.window(EventTimeSessionWindows.withGap(Time.milliseconds(500)))

        windowStream.reduce((text1, text2) => (text1._1, 0L, text1._3 + text2._3)).map(_._3)
          .print("windows:::")
          .setParallelism(1)

        env.execute("EventTime Session Windows")
    }
}