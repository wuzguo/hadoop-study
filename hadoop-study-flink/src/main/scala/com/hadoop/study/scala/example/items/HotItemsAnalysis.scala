package com.hadoop.study.scala.example.items

import com.hadoop.study.scala.example.beans.{ItemViewCount, UserBehavior}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp
import scala.collection.mutable.ListBuffer

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/21 19:15
 */

object HotItemsAnalysis {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(2)

        // 读取数据
        val inputStream = env.socketTextStream("hadoop003", 9999)

        // 行为数据，加上WaterMarker的数据，由于数据是ETL数据，已经排好序
        val dataStream = inputStream.map(line => {
            val values = line.split(",")
            UserBehavior(values(0).toLong, values(1).toLong, values(2).toInt, values(3), new Timestamp(values(4).toLong * 1000))
        }).assignAscendingTimestamps(_.timestamp.getTime)
        // 打印数据
        dataStream.print("data: ")

        // 开窗，聚合
        val aggStream = dataStream.filter(_.action == "pv")
          .keyBy(_.itemId)
          .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
          .aggregate(new AggregatorFunction, new ResultWindowFunction)

        // 打印聚合结果
        aggStream.print("agg: ")

        // 计算TopN
        val resultStream = aggStream.keyBy(_.windowEnd).process(new TopItemsFunction(5))
        resultStream.print()

        // 执行
        env.execute("Hot Items Analysis")
    }

    // 预聚合函数
    class AggregatorFunction extends AggregateFunction[UserBehavior, Long, Long] {

        override def createAccumulator(): Long = 0

        override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

        override def getResult(accumulator: Long): Long = accumulator

        override def merge(a: Long, b: Long): Long = a + b
    }

    // 结果函数
    class ResultWindowFunction extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {

        override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
            out.collect(ItemViewCount(key, window.getEnd, new Timestamp(window.getEnd), input.iterator.next()))
        }
    }

    class TopItemsFunction(val topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
        // 先定义状态：ListState
        private var viewCountState: ListState[ItemViewCount] = _

        override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
            // 将数据放在ListState
            viewCountState.add(value)
            // 打印当前Watermark时间
            println(s"Watermark: ${new Timestamp(ctx.timerService().currentWatermark())}")
            // 注册一个定时器
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
            // 转换
            val itemViewCounts: ListBuffer[ItemViewCount] = ListBuffer()
            viewCountState.get().forEach(_ => itemViewCounts += _)

            // 倒序排序，取前几名
            val topViewCounts = itemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

            // 输出：将排名信息格式化成String，便于打印输出可视化展示
            val builder = new StringBuilder
            builder.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n").append("-----------------------------------\n")
            // 遍历结果列表中的每个ItemViewCount，输出到一行
            for (i <- topViewCounts.indices) {
                val itemViewCount = topViewCounts(i)
                builder.append("No.").append(i + 1).append(":\t")
                  .append("商品 = ").append(itemViewCount.itemId).append("\t")
                  .append("热度 = ").append(itemViewCount.count).append("\n")
            }
            builder.append("===================================\n")
            // 打印结果
            out.collect(builder.toString)
            // 清空
            viewCountState.clear()
        }

        override def open(parameters: Configuration): Unit = {
            viewCountState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-view-count", classOf[ItemViewCount]))
        }
    }
}
