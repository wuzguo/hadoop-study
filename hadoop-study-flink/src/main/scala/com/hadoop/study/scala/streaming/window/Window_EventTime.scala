package com.hadoop.study.scala.streaming.window

import com.hadoop.study.scala.streaming.beans.Sensor
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/9 14:44
 */

object Window_EventTime {

    def main(args: Array[String]): Unit = {
        // 1. 获取环境配置
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.getConfig.setAutoWatermarkInterval(100)

        // 2. 从Socket读取文件
        val dss: DataStream[String] = env.readTextFile("./hadoop-study-datas/flink/core/sensor.txt")

        // 3. 转换成Sensor类型，分配时间戳和watermark
        val dsSensor: DataStream[Sensor] = dss.map(line => {
            val values = line.split(",")
            Sensor(values(0), values(1).trim.toLong, values(2).trim.toDouble)
        }).assignTimestampsAndWatermarks(
            WatermarkStrategy.forBoundedOutOfOrderness[Sensor](Duration.ofSeconds(1))
              .withTimestampAssigner(new SerializableTimestampAssigner[Sensor] {
                  override def extractTimestamp(element: Sensor, recordTimestamp: Long): Long = element.timestamp * 1000
              }))

        // output
        val output = new OutputTag[Sensor]("late")

        val sensorStream: DataStream[Sensor] = dsSensor.keyBy(_.id)
          // 滚动窗口2秒
          .window(TumblingEventTimeWindows.of(Time.seconds(15)))
          .allowedLateness(Time.minutes(1))
          .sideOutputLateData(output)
          .minBy("temp")

        // 最小温度
        sensorStream.print("min temp")

        sensorStream.getSideOutput(output).print("late")

        env.execute("Streaming EventTime Window")
    }
}
