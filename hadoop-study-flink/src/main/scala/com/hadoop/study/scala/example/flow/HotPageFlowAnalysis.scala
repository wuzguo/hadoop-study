package com.hadoop.study.scala.example.flow

import com.hadoop.study.scala.example.beans.{PageViewCount, PageViewEvent}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter
import org.apache.flink.util.Collector

import java.sql.Timestamp
import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/21 19:17
 */

object HotPageFlowAnalysis {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 读取数据
        val inputStream = env.readTextFile("./hadoop-study-datas/flink/input/apache.txt")

        // 转换为对象，加上Watermark
        val dataStream = inputStream.map(line => {
            val values = line.split(" ")
            // 对事件时间进行转换，得到时间戳
            val dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
            val timestamp = dateFormat.parse(values(3)).getTime
            PageViewEvent(values(0), timestamp, values(5), values(6))
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy(new TimestampExtractor(Time.seconds(1))))

        // 打印数据
        dataStream.print("data: ")
        // Side Output
        val lateOutPutTag = new OutputTag[PageViewEvent]("late")
        // 进行开窗聚合，以及排序输出
        val filterUrl = Array(".ico", ".css", ".png", ".jpg", ".js", ".jar", ".html", ".conf", ".ttf", ".jpeg", ".gif", ".xml", ".log", ".txt")
        val aggStream = dataStream.filter(event => {
            // 获取最后一个点
            val indexOf = event.url.lastIndexOf(".")
            if (indexOf <= 0) {
                event.method == "GET"
            } else {
                event.method == "GET" && !filterUrl.contains(event.url.substring(indexOf, event.url.length))
            }
        }).keyBy(_.url)
          .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
          .allowedLateness(Time.minutes(1))
          .sideOutputLateData(lateOutPutTag)
          .aggregate(new AggregatorFunction, new ResultWindowFunction)

        aggStream.print("agg: ")
        // 打印迟到的数据
        aggStream.getSideOutput(lateOutPutTag).print("late: ")

        // 结果
        val resultStream = aggStream.keyBy(_.windowEnd).process(new TopPagesFunction(5))

        // 打印结果
        resultStream.print()
        env.execute("Hot Page Flow Analysis")
    }


    class TimestampExtractor(maxOutOfOrderness: Time) extends BoundedOutOfOrdernessTimestampExtractor[PageViewEvent](maxOutOfOrderness: Time) {

        override def extractTimestamp(element: PageViewEvent): Long = element.timestamp
    }

    // 预聚合函数
    class AggregatorFunction extends AggregateFunction[PageViewEvent, Long, Long] {

        override def createAccumulator(): Long = 0

        override def add(value: PageViewEvent, accumulator: Long): Long = accumulator + 1

        override def getResult(accumulator: Long): Long = accumulator

        override def merge(a: Long, b: Long): Long = a + b
    }

    class ResultWindowFunction extends WindowFunction[Long, PageViewCount, String, TimeWindow] {

        override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
            out.collect(PageViewCount(key, window.getEnd, input.iterator.next()))
        }
    }

    class TopPagesFunction(val topSize: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {

        // 先定义状态：ListState
        private var viewCountState: ListState[PageViewCount] = _

        override def open(parameters: Configuration): Unit = {
            viewCountState = getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("page-view-count", classOf[PageViewCount]))
        }

        override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {
            viewCountState.add(value)
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
            // 转换
            val pageViewCounts: ListBuffer[PageViewCount] = ListBuffer()
            viewCountState.get().forEach(pageViewCount => pageViewCounts += pageViewCount)

            // 倒序排序，取前几名
            val topViewCounts = pageViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

            // 输出：将排名信息格式化成String，便于打印输出可视化展示
            val builder = new StringBuilder
            builder.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n").append("-----------------------------------\n")
            // 遍历结果列表中的每个ItemViewCount，输出到一行
            for (i <- topViewCounts.indices) {
                val itemViewCount = topViewCounts(i)
                builder.append("No.").append(i + 1).append(":\t")
                  .append("URL = ").append(itemViewCount.url).append("\t")
                  .append("热度 = ").append(itemViewCount.count).append("\n")
            }
            builder.append("===================================\n")
            // 打印结果
            Thread.sleep(1000)
            out.collect(builder.toString)
        }
    }
}
