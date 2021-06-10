package com.hadoop.study.scala.streaming.state

import com.hadoop.study.scala.streaming.beans.Sensor
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/10 15:42
 */

object State_KeyedState {

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
        val resultStream = sensorStream.keyBy(_.id).map(new CustomRichMapFunction)
        resultStream.print("result ")

        env.execute("Streaming State KeyedState")
    }

    class CustomRichMapFunction extends RichMapFunction[Sensor, Int] {

        var keyCountState: ValueState[Int] = _

        override def map(value: Sensor): Int = {
            var count: Int = keyCountState.value
            count += 1
            keyCountState.update(count)
            count
        }

        override def open(parameters: Configuration): Unit = {
            keyCountState = getRuntimeContext.getState(new ValueStateDescriptor[Int]("key-count", classOf[Int]))
            super.open(parameters)
        }

        override def close(): Unit = {
            keyCountState.clear()
            super.close()
        }
    }
}
