package com.hadoop.study.scala.example.order

import com.hadoop.study.scala.example.beans.OrderEvent
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time

import java.util

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/21 19:17
 */

object OrderPayDetectWithCEPAnalysis {

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

        // 定义CEP
        val orderPattern = Pattern
          .begin[OrderEvent]("create").where(_.action == "create")
          .followedBy("pay").where(_.action == "pay")
          .within(Time.minutes(15))

        // 2. 将模式应用到数据流上，得到一个PatternStream
        val patternStream = CEP.pattern(dataStream.keyBy(_.orderId), orderPattern)

        // 3. 选择器
        val resultStream = patternStream.select(outputTag, new OrderTimeoutFunction, new OrderPayedFunction)

        // 4. 打印
        resultStream.print("payed")
        resultStream.getSideOutput(outputTag).print("nopay")

        // 5. 执行
        env.execute("Order Pay Detect Witch CEP Analysis")
    }

    // 实现自定义的PatternTimeoutFunction
    class OrderTimeoutFunction extends PatternTimeoutFunction[OrderEvent, String] {

        override def timeout(pattern: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): String = {
            val timeoutOrderId = pattern.get("create").iterator().next().orderId
            s"订单:${timeoutOrderId} 超过十五分钟未支付"
        }
    }

    // 实现自定义的PatternSelectFunction
    class OrderPayedFunction extends PatternSelectFunction[OrderEvent, String] {

        override def select(pattern: util.Map[String, util.List[OrderEvent]]): String = {
            val payedOrderId = pattern.get("pay").iterator().next().orderId
            s"订单:${payedOrderId} 支付成功"
        }
    }
}
