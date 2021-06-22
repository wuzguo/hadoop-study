package com.hadoop.study.scala.example.flow

import com.hadoop.study.scala.example.beans.UserBehavior
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/21 19:15
 */

object UniqueVisitorAnalysis {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        // 读取数据
        val inputStream = env.readTextFile("./hadoop-study-datas/flink/input/UserBehavior.csv")

        // 行为数据，加上WaterMarker的数据，由于数据是ETL数据，已经排好序
        val dataStream = inputStream.map(line => {
            val values = line.split(",")
            UserBehavior(values(0).toLong, values(1).toLong, values(2).toInt, values(3), new Timestamp(values(4).toLong * 1000))
        }).assignAscendingTimestamps(_.timestamp.getTime)

        //  计算
        val resultStream = dataStream.filter(_.action == "pv")
          .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
          .apply(new UniqueAllWindow)

        resultStream.print()

        // 执行
        env.execute("Unique Visitor Analysis")
    }

    class UniqueAllWindow extends AllWindowFunction[UserBehavior, String, TimeWindow] {

        override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[String]): Unit = {

            // 定义一个Set
            var userIds = Set[Long]()
            // 遍历窗口中的所有数据，把userId添加到set中，自动去重
            input.foreach(userBehavior => userIds += userBehavior.userId)
            // 将set的size作为去重后的uv值输出
            out.collect(s"时间：${window.getEnd} ，数量：${userIds.size}")
        }
    }
}
