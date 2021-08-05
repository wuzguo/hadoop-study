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

object Process_ApplicationCase {

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

        // 测试KeyedProcessFunction，先分组然后自定义处理
        sensorStream.keyBy(_.id)
          .process(new TempIncrWarningProcessFunction(10))
          .print()

        // 执行
        env.execute("Streaming Process Application")
    }

    class TempIncrWarningProcessFunction(val interval: Int) extends KeyedProcessFunction[String, Sensor, String] {

        private var timerState: ValueState[Long] = _

        private var lastTempState: ValueState[Double] = _

        override def open(parameters: Configuration): Unit = {

            lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp-state", classOf[Double]))

            timerState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts-state", classOf[Long]))

            super.open(parameters)
        }

        override def processElement(value: Sensor, ctx: KeyedProcessFunction[String, Sensor, String]#Context,
                                    out: Collector[String]): Unit = {
            // 取出状态
            val lastTemp = lastTempState.value
            val timer = timerState.value

            // 如果温度上升并且没有定时器，注册10秒后的定时器，开始等待
            if (value.temp > lastTemp && timer == 0) {
                // 计算出定时器时间戳。这里使用的处理时间
                val timestamp = ctx.timerService().currentProcessingTime() + interval * 1000
                // 注册定时器，当前处理时间延时10秒触发
                ctx.timerService.registerProcessingTimeTimer(timestamp)

                timerState.update(timestamp)
            } else {
                // 如果温度下降，那么删除定时器
                if (value.temp < lastTemp && timer != 0) {
                    ctx.timerService.deleteProcessingTimeTimer(timer)
                    timerState.clear()
                }
            }

            // 更新温度状态
            lastTempState.update(value.temp)
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Sensor, String]#OnTimerContext,
                             out: Collector[String]): Unit = {
            out.collect("传感器: " + ctx.getCurrentKey + " 温度值连续 " + interval + "s 上升, 当前温度: " + lastTempState.value())
            timerState.clear()
            super.onTimer(timestamp, ctx, out)
        }

        override def close(): Unit = {
            timerState.clear()
            super.close()
        }
    }
}
