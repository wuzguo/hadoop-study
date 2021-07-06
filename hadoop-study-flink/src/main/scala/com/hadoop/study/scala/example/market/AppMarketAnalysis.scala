package com.hadoop.study.scala.example.market

import com.hadoop.study.scala.example.beans.{MarketUserBehavior, MarketViewCount}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp
import java.util.UUID
import scala.util.Random

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/21 19:16
 */

object AppMarketAnalysis {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 源数据
        val dataStream = env.addSource(new SimulatedSourceFunction).assignAscendingTimestamps(_.timestamp)

        // 开窗统计输出
        val resultStream = dataStream
          .filter(_.action != "uninstall")
          .keyBy(data => (data.channel, data.action))
          .window(SlidingEventTimeWindows.of(Time.days(1), Time.seconds(5)))
          .process(new MarketCountProcessFunction)

        resultStream.print()

        // 执行
        env.execute("App Market Analysis")
    }

    // 数据源
    class SimulatedSourceFunction extends RichSourceFunction[MarketUserBehavior] {

        // 定义用户行为和渠道的集合
        private val actions: Seq[String] = Seq("view", "download", "install", "uninstall")
        private val channels: Seq[String] = Seq("appstore", "weibo", "wechat", "tieba")
        private val random: Random = Random
        private var running = true

        override def run(ctx: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {

            // 定义一个生成数据最大的数量
            val maxCounts = Long.MaxValue

            // 计数
            var count = 0

            while (running && count < maxCounts) {
                val id = UUID.randomUUID().toString
                val action = actions(random.nextInt(actions.size))
                val channel = channels(random.nextInt(channels.size))
                val timestamp = System.currentTimeMillis()
                ctx.collect(MarketUserBehavior(id, action, channel, timestamp))
                count += 1
                Thread.sleep(50)
            }

        }

        override def cancel(): Unit = running = false
    }

    class MarketCountProcessFunction extends ProcessWindowFunction[MarketUserBehavior, MarketViewCount, (String, String), TimeWindow] {

        override def process(key: (String, String), context: Context, elements: Iterable[MarketUserBehavior], out: Collector[MarketViewCount]): Unit = {
            // 窗口起止时间
            val start = new Timestamp(context.window.getStart).toString
            val end = new Timestamp(context.window.getEnd).toString
            val channel = key._1
            val action = key._2
            val count = elements.size
            out.collect(MarketViewCount(start, end, channel, action, count))
        }
    }
}
