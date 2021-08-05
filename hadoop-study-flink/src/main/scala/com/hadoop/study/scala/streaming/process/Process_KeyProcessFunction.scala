package com.hadoop.study.scala.streaming.process

import com.hadoop.study.scala.streaming.beans.Sensor
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/10 11:38
 */

object Process_KeyProcessFunction {

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

        // 测试KeyedProcessFunction，先分组然后自定义处理
        sensorStream.keyBy(_.id).process(new CustomProcessFunction).print()
        // 执行
        env.execute("Streaming Process KeyProcessFunction")
    }

    class CustomProcessFunction extends KeyedProcessFunction[String, Sensor, Integer] {

        private var timerState: ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
            timerState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-state", classOf[Long]))
            super.open(parameters)
        }

        override def processElement(value: Sensor, ctx: KeyedProcessFunction[String, Sensor, Integer]#Context,
                                    out: Collector[Integer]): Unit = {
            out.collect(value.id.length)
            ctx.timerService.registerProcessingTimeTimer(ctx.timerService.currentProcessingTime + 5000)
            timerState.update(ctx.timerService().currentProcessingTime() + 1000)
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Sensor, Integer]#OnTimerContext,
                             out: Collector[Integer]): Unit = {
            println(timestamp + " 定时器触发")
            ctx.getCurrentKey
            ctx.timeDomain()

            super.onTimer(timestamp, ctx, out)
        }

        override def close(): Unit = {
            timerState.clear()
            super.close()
        }
    }
}
