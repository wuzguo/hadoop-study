package com.hadoop.study.scala.example.order

import com.hadoop.study.scala.example.beans.{OrderEvent, ReceiptEvent}
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/25 8:56
 */

object OrderReconciliationWithJoinAnalysis {

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
        val resultStream = orderEventStream.intervalJoin(receiptEventStream)
          .between(Time.seconds(-10), Time.seconds(20))
          .process(new OrderReconciliationJoinFunction)

        resultStream.print()
        env.execute("Order Reconciliation With Join Analysis")
    }

    class OrderReconciliationJoinFunction extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
        override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
            out.collect((left, right))
        }
    }
}
