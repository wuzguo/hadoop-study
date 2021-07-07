package com.hadoop.study.scala.streaming.state

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/7 14:41
 */

object State_BroadcastState {

    def main(args: Array[String]): Unit = {
        // 环境变量
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置并行度
        env.setParallelism(1)

        val oneStream = env.fromSequence(0, 5).assignTimestampsAndWatermarks(
            WatermarkStrategy.forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner[Long] {
                override def extractTimestamp(element: Long, recordTimestamp: Long): Long = element
            }))

        val collections = List("hadoop:1", "spark:2", "flink:3", "storm:4", "hive:5", "hdfs:6")
        val twoStream = env.fromCollection(collections).assignTimestampsAndWatermarks(
            WatermarkStrategy.forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner[String] {
                override def extractTimestamp(element: String, recordTimestamp: Long): Long = element.split(":")(1).toLong
            }))

        val descriptor = new MapStateDescriptor[Long, String]("broadcast-state", classOf[Long], classOf[String])
        val broadcast = twoStream.broadcast(descriptor)

        // the timestamp should be high enough to trigger the timer after all the elements arrive.
        val output = oneStream.connect(broadcast).process(new CustomBroadcastProcessFunction(descriptor))
        output.print.setParallelism(1)

        env.execute("Streaming State BroadcastState")
    }

    class CustomBroadcastProcessFunction(descriptor: MapStateDescriptor[Long, String]) extends BroadcastProcessFunction[Long, String, String] {

        override def processElement(key: Long, ctx: BroadcastProcessFunction[Long, String, String]#ReadOnlyContext, out: Collector[String]): Unit = {
            val value = ctx.getBroadcastState(descriptor).get(key)
            if (value != null) {
                out.collect(s"${key}: ${value}")
            }
        }

        override def processBroadcastElement(value: String, ctx: BroadcastProcessFunction[Long, String, String]#Context, out: Collector[String]): Unit = {
            val values = value.split(":")
            ctx.getBroadcastState(descriptor).put(values(1).toLong, values(0))
        }
    }
}
