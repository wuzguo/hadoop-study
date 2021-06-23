package com.hadoop.study.scala.example.market

import com.hadoop.study.scala.example.beans.{AdClickEvent, AdClickProvinceCount}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/23 10:24
 */

object AdClickAnalysis {

    def main(args: Array[String]): Unit = {
        // 获取运行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 读取源文件
        val inputStream = env.readTextFile("./hadoop-study-datas/flink/input/AdClickLog.csv")

        val dataStream = inputStream.map(line => {
            val values = line.split(",")
            AdClickEvent(values(0).toLong, values(1).toLong, values(2), values(3), values(4).toLong * 1000)
        }).assignAscendingTimestamps(_.timestamp)

        // 输出结果
        val outputTag = new OutputTag[String]("user-black-tag")
        val filteredResultStream = dataStream.keyBy(data => (data.userId, data.adId))
          .process(new UserClickProcessFunction(100, tagId))
        filteredResultStream.getSideOutput(outputTag).print("black")

        // 统计每个小时的广告点击量，按省份分组
        val clickCountStream = filteredResultStream.keyBy(_.province)
          .window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(5)))
          .aggregate(new AggregatorFunction, new ResultWindowFunction)

        clickCountStream.print("click")
        // 执行结果
        env.execute("Ad Click Analysis")
    }

    class UserClickProcessFunction(maxCount: Int, outputTag: OutputTag[String]) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent] {
        // （广告ID，数量）
        private var adClickState: MapState[Long, Int] = _

        // 黑名单状态
        private var blackListState: ValueState[Boolean] = _

        override def open(parameters: Configuration): Unit = {
            adClickState = getRuntimeContext.getMapState(new MapStateDescriptor[Long, Int]("ad-click-state", classOf[Long], classOf[Int]))
            blackListState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("black-list-state", classOf[Boolean]))
        }

        override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent,
          AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
            // 获取次数
            val count = adClickState.get(value.adId)
            adClickState.put(ctx.getCurrentKey._2, count + 1)

            // 判断只要是第一个数据来了，直接注册0点的清空状态定时器
            if (count == 0) {
                // 24点清空
                val clearTimestamp = (value.timestamp / (1000 * 60 * 60 * 24) + 1) * (24 * 60 * 60 * 1000)
                ctx.timerService().registerEventTimeTimer(clearTimestamp)
            }

            // 是不是在黑名单中
            val isBlackList = blackListState.value()

            // 计算是不是满足黑名单的条件了
            var curCount = 0
            adClickState.values().forEach(count => curCount += count)
            if (curCount >= maxCount && !isBlackList) {
                blackListState.update(true)
                ctx.output(outputTag, s"用户：${ctx.getCurrentKey._1}，点击广告超过 ${curCount} 次，已被拉入黑名单")
            }

            // 输出结果
            out.collect(value)
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent,
          AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
            adClickState.clear()
            blackListState.clear()
        }
    }

    class ClickCountProcessFunction extends KeyedProcessFunction[String, AdClickEvent, String] {
        override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[String, AdClickEvent,
          String]#Context, out: Collector[String]): Unit = {

        }
    }

    class AggregatorFunction extends AggregateFunction[AdClickEvent, Long, Long] {

        override def createAccumulator(): Long = 0

        override def add(value: AdClickEvent, accumulator: Long): Long = accumulator + 1

        override def getResult(accumulator: Long): Long = accumulator

        override def merge(a: Long, b: Long): Long = a + b
    }

    class ResultWindowFunction extends WindowFunction[Long, AdClickProvinceCount, String, TimeWindow] {
        override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdClickProvinceCount]): Unit = {
            out.collect(AdClickProvinceCount(new Timestamp(window.getEnd), key, input.head))
        }
    }
}
