package com.hadoop.study.scala.streaming.window

import com.hadoop.study.scala.streaming.beans.Sensor
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/9 14:44
 */

object Window_Count {

    def main(args: Array[String]): Unit = {
        // 1. 获取环境配置
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.getConfig.setAutoWatermarkInterval(100)

        // 2. 从Socket读取文件
        val dss: DataStream[String] = env.readTextFile("./hadoop-study-datas/flink/core/sensor.txt")

        // 3. 转换成Sensor类型，CountWindow 跟时间无关
        val sensors = dss.map(line => {
            val values = line.split(",")
            Sensor(values(0), values(1).trim.toLong, values(2).trim.toDouble)
        }).keyBy(_.id)
          .countWindow(10, 2)
          .aggregate(new AvgTempFunction)

        // 4. 打印
        sensors.print("avg temp")

        env.execute("Streaming Count Window")
    }

    // 自定义 timestamp extractor
    class AvgTempFunction extends AggregateFunction[Sensor, (Double, Integer), Double] {

        override def createAccumulator(): (Double, Integer) = (0.0, 0)

        override def add(value: Sensor, accumulator: (Double, Integer)): (Double, Integer) = (value.temp +
          accumulator._1, accumulator._2 + 1)

        override def getResult(accumulator: (Double, Integer)): Double = accumulator._1 / accumulator._2

        override def merge(pre: (Double, Integer), next: (Double, Integer)): (Double, Integer) =
            (pre._1 + next._1, pre._2 + next._2)
    }

}
