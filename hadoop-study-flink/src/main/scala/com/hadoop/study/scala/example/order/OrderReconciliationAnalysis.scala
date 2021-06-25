package com.hadoop.study.scala.example.order

import com.hadoop.study.scala.example.beans.{OrderEvent, ReceiptEvent}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/25 8:56
 */

object OrderReconciliationAnalysis {

    def main(args: Array[String]): Unit = {
        // 获取运行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 读取源文件
        val orderInputStream = env.readTextFile("./hadoop-study-datas/flink/input/OrderLog.csv")

        val orderDataStream = orderInputStream.map(line => {
            val values = line.split(",")
            OrderEvent(values(0).toLong, values(1), values(2), values(3).toLong * 1000)
        }).assignAscendingTimestamps(_.timestamp)

        val orderEventStream = orderDataStream.filter(_.action == "pay").keyBy(_.txId)

        val receiptInputStream = env.readTextFile("./hadoop-study-datas/flink/input/ReceiptLog.csv")

        val receiptDataStream = receiptInputStream.map(line => {
            val values = line.split(",")
            ReceiptEvent(values(0), values(1), values(2).toLong * 1000)
        }).assignAscendingTimestamps(_.timestamp)

        val receiptEventStream = receiptDataStream.keyBy(_.txId)

        // 3. 合并两条流，进行处理
        val unmatchedPayOutputTag = new OutputTag[OrderEvent]("unmatched-pay")
        val unmatchedReceiptOutputTag = new OutputTag[ReceiptEvent]("unmatched-receipt")

        val resultStream = orderEventStream.connect(receiptEventStream)
          .process(new OrderReconciliationProcessFunction(unmatchedPayOutputTag, unmatchedReceiptOutputTag))

        resultStream.print("matched")
        resultStream.getSideOutput(unmatchedPayOutputTag).print("unmatched pays")
        resultStream.getSideOutput(unmatchedReceiptOutputTag).print("unmatched receipts")

        env.execute("Order Reconciliation Analysis")
    }

    class OrderReconciliationProcessFunction(unmatchedPayOutputTag: OutputTag[OrderEvent], unmatchedReceiptOutputTag: OutputTag[ReceiptEvent])
      extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {

        private var payEventState: ValueState[OrderEvent] = _

        private var receiptEventState: ValueState[ReceiptEvent] = _

        override def processElement1(value: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent,
          (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
            // 订单支付来了，要判断之前是否有到账事件
            val receipt = receiptEventState.value()
            if (receipt != null) {
                // 如果已经有receipt，正常输出匹配，清空状态
                out.collect((value, receipt))
                receiptEventState.clear()
                payEventState.clear()
            } else {
                // 如果还没来，注册定时器开始等待5秒
                ctx.timerService().registerEventTimeTimer(value.timestamp + 5000)
                // 更新状态
                payEventState.update(value)
            }
        }

        override def processElement2(value: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent,
          (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

            // 到账事件来了，要判断之前是否有pay事件
            val pay = payEventState.value()
            if (pay != null) {
                // 如果已经有pay，正常输出匹配，清空状态
                out.collect((pay, value))
                receiptEventState.clear()
                payEventState.clear()
            } else {
                // 如果还没来，注册定时器开始等待3秒
                ctx.timerService().registerEventTimeTimer(value.timestamp + 3000)
                // 更新状态
                receiptEventState.update(value)
            }
        }

        override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent,
          (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
            // 定时器触发，判断状态中哪个还存在，就代表另一个没来，输出到侧输出流
            if (payEventState.value() != null) {
                ctx.output(unmatchedPayOutputTag, payEventState.value())
            }
            if (receiptEventState.value() != null) {
                ctx.output(unmatchedReceiptOutputTag, receiptEventState.value())
            }
            // 清空状态
            receiptEventState.clear()
            payEventState.clear()
        }

        override def open(parameters: Configuration): Unit = {
            payEventState = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-event-state", classOf[OrderEvent]))
            receiptEventState = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-event-state", classOf[ReceiptEvent]))
        }
    }

}
