package com.hadoop.study.scala.streaming.transform

import com.hadoop.study.scala.streaming.beans.Sensor
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/10 11:38
 */

object Transform_MultiStreams {

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

        // 1. 分流，按照温度值30度分值分为两条流
        val highOutput = OutputTag[Sensor]("side-output-high")
        val lowOutput = OutputTag[Sensor]("side-output-low")

        val mainStream = sensorStream.process((value: Sensor, ctx: ProcessFunction[Sensor, Sensor]#Context, out: Collector[Sensor]) => {
            // 发送数据到主要的输出
            out.collect(value)

            // 发送数据到旁路输出
            if (value.temp > 30.0) {
                ctx.output(highOutput, value)
            } else {
                ctx.output(lowOutput, value)
            }
        })

        val highStream = mainStream.getSideOutput(highOutput)
      //  highStream.print("high side ")

        // 2. 合流 connect，将高温流转换成二元组类型，与低温流连接合并之后，输出状态信息
        val highTupleStream = highStream.map(new MapFunction[Sensor, (String, Double)] {
            override def map(value: Sensor): (String, Double) = {
                (value.id, value.temp)
            }
        })

        val lowStream = mainStream.getSideOutput(lowOutput)

        val connectStreams = highTupleStream.connect(lowStream)

        val resultStream = connectStreams.map(new CoMapFunction[(String, Double), Sensor, (String, Double, String)] {

            override def map1(value: (String, Double)): (String, Double, String) = (value._1, value._2, "high temp warning")

            override def map2(value: Sensor): (String, Double, String) = (value.id, value.temp, "normal temp")
        })

       // resultStream.print("result ")

        // 3. union联合多条流
        val unionStream = highStream.union(lowStream, sensorStream)
       // unionStream.print("union ")

        env.execute("Streaming Transform MultiStream")
    }
}
