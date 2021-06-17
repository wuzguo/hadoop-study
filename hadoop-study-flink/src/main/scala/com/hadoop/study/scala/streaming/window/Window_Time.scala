package com.hadoop.study.scala.streaming.window

import com.hadoop.study.scala.streaming.beans.Sensor
import com.hadoop.study.scala.streaming.window.Window_EventTime.TimestampExtractor
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter
import org.apache.flink.util.Collector

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/9 14:44
 */

object Window_Time {

    def main(args: Array[String]): Unit = {
        // 1. 获取环境配置
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // 2. 从Socket读取文件
        val dss: DataStream[String] = env.readTextFile("./hadoop-study-datas/flink/core/sensor.txt")

        // 3. 转换成Sensor类型，分配时间戳和watermark
        val dataStream: DataStream[Sensor] = dss.map(line => {
            val values = line.split(",")
            Sensor(values(0), values(1).trim.toLong, values(2).trim.toDouble)
        }).assignTimestampsAndWatermarks(
            new AssignerWithPeriodicWatermarksAdapter.Strategy(new TimestampExtractor(Time.seconds(1))))

        val sensors: DataStream[Integer] = dataStream.keyBy(_.id)
          // 滚动窗口15秒
          .window(TumblingEventTimeWindows.of(Time.seconds(15)))
          .aggregate(new IncrementFunction)

        sensors.print("incr")


        // 4. 全窗口函数
        val allStream: DataStream[(String, Long, Integer)] = dataStream.keyBy(_.id)
          // 滚动窗口15秒
          .window(TumblingEventTimeWindows.of(Time.seconds(15)))
          .apply(new AllWindowFunction)

        allStream.print("all")

        // 5. 其他API
        val output = new OutputTag[Sensor]("late")
        val sumStream: DataStream[Sensor] = dataStream.keyBy(_.id)
          .window(TumblingEventTimeWindows.of(Time.seconds(15)))
          .allowedLateness(Time.minutes(1))
          .sideOutputLateData(output)
          .sum("temp")
        sumStream.getSideOutput(output).print("late");

        sumStream.print("sum")

        env.execute("Streaming Time Window")
    }

    class AllWindowFunction extends WindowFunction[Sensor, (String, Long, Integer), String, TimeWindow] {
        override def apply(key: String, window: TimeWindow, input: Iterable[Sensor], out: Collector[(String, Long, Integer)]): Unit = {
            val windowEnd = window.getEnd
            out.collect((key, windowEnd, input.toList.size))
        }
    }


    // 自定义 增量聚合函数
    class IncrementFunction extends AggregateFunction[Sensor, Integer, Integer] {

        override def createAccumulator(): Integer = 0

        override def add(value: Sensor, accumulator: Integer): Integer = accumulator + 1

        override def getResult(accumulator: Integer): Integer = accumulator

        override def merge(pre: Integer, next: Integer): Integer = pre + next
    }
}
