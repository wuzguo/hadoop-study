package com.hadoop.study.scala.streaming.state

import com.hadoop.study.scala.streaming.beans.Sensor
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/10 15:42
 */

object State_KeyedState_Case {

    def main(args: Array[String]): Unit = {
        // 环境变量
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置并行度
        env.setParallelism(1)

        // 从文件读取数据
        val socketStream = env.socketTextStream("hadoop003", 9999)

        // 构造Stream
        val sensorStream = socketStream.map(line => {
            val values = line.split(",")
            Sensor(values(0), values(1).trim.toLong, values(2).trim.toDouble)
        })

        // 定义一个有状态的map操作，统计当前分区数据个数
        val resultStream = sensorStream.keyBy(_.id).flatMap(new CustomRichMapFunction(10.0))
        resultStream.print("result ")

        env.execute("Streaming State KeyedState Case")
    }

    class CustomRichMapFunction(val threshold: Double) extends RichFlatMapFunction[Sensor, (String, Double, Double)] {

        // 定义状态，保存上一次的温度值
        var lastTempState: ValueState[Double] = _

        override def flatMap(value: Sensor, out: Collector[(String, Double, Double)]): Unit = {
            // 获取状态
            val lastTemp = lastTempState.value

            // 如果状态不为null，那么就判断两次温度差值
            val diff = Math.abs(value.temp - lastTemp)
            if (diff >= threshold) {
                out.collect((value.id, lastTemp, value.temp))
            }

            // 更新状态
            lastTempState.update(value.temp)
        }

        override def open(parameters: Configuration): Unit = {
            lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp-state", classOf[Double]))
            super.open(parameters)
        }

        override def close(): Unit = {
            lastTempState.clear()
            super.close()
        }
    }
}
