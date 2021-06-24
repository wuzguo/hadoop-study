package com.hadoop.study.scala.example.order

import com.hadoop.study.scala.example.beans.OrderEvent
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/21 19:17
 */

object OrderPayDetectAnalysis {

    def main(args: Array[String]): Unit = {
        // 获取运行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 读取源文件
        val inputStream = env.readTextFile("./hadoop-study-datas/flink/input/OrderLog.csv")

        val dataStream = inputStream.map(line => {
            val values = line.split(",")
            OrderEvent(values(0).toLong, values(1), values(2), values(3).toLong * 1000)
        }).assignAscendingTimestamps(_.timestamp)

        // 根据订单ID聚合
        val outputTag = new OutputTag[String]("output-tag-no-pay")
        val resultStream = dataStream.keyBy(_.orderId).process(new PayDetectProcessFunction(outputTag))

        resultStream.print("payed")
        resultStream.getSideOutput(outputTag).print("nopay")

        // 执行
        env.execute("Order Pay Detect Analysis")
    }

    class PayDetectProcessFunction(outputTag: OutputTag[String]) extends KeyedProcessFunction[Long, OrderEvent, String] {

        private var orderValueState: MapState[String, Long] = _

        private var orderTimestampState: ValueState[Long] = _

        override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, String]#Context,
                                    out: Collector[String]): Unit = {
            // 添加到列表
            orderValueState.put(value.action, value.timestamp)

            if (value.action == "create") {
                // 注册15分钟后触发的定时器
                ctx.timerService().registerEventTimeTimer(value.timestamp + 15 * 60 * 1000)
            }
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, String]#OnTimerContext,
                             out: Collector[String]): Unit = {
            val createTime = orderValueState.get("create")
            val payTime = orderValueState.get("pay")
            if (payTime == 0 || (payTime - createTime > 15 * 60 * 1000)) {
                ctx.output(outputTag, s"订单:${ctx.getCurrentKey} 超过十五分钟未支付")
            } else {
                out.collect(s"订单:${ctx.getCurrentKey} 支付成功")
            }

            // 删除定时器
            ctx.timerService().deleteEventTimeTimer(orderTimestampState.value())

            // 清空
            orderTimestampState.clear()
            orderValueState.clear()
        }

        override def open(parameters: Configuration): Unit = {
            orderValueState = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("order-value-state", classOf[String], classOf[Long]))
            orderTimestampState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("order-timestamp-state", classOf[Long]))
        }
    }
}
