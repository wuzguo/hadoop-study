package com.hadoop.study.scala.example.login

import com.hadoop.study.scala.example.beans.UserLoginEvent
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/21 19:15
 */

object LoginFailDetectAnalysis {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream = env.readTextFile("./hadoop-study-datas/flink/input/LoginLog.csv")

        val dataStream = inputStream.map(line => {
            val values = line.split(",")
            UserLoginEvent(values(0).toLong, values(1), values(2), values(3).toLong * 1000)
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy(new TimestampExtractor(Time.seconds(2))))

        // 聚合处理
        val outputTag = new OutputTag[String]("login-fail-tag")
        val resultStream = dataStream.filter(_.result == "fail")
          .keyBy(_.userId)
          .process(new LoginDetectProcessFunction(2, outputTag))

        // 侧输出
        resultStream.getSideOutput(outputTag).print("warning")

        // 执行
        env.execute("Login Fail Detect Analysis")
    }

    // 自定义 timestamp extractor
    class TimestampExtractor(maxOutOfOrderness: Time) extends BoundedOutOfOrdernessTimestampExtractor[UserLoginEvent](maxOutOfOrderness: Time) {

        override def extractTimestamp(element: UserLoginEvent): Long = element.timestamp
    }

    class LoginDetectProcessFunction(times: Int, outputTag: OutputTag[String]) extends KeyedProcessFunction[Long, UserLoginEvent, UserLoginEvent] {

        // 状态
        private var loginFailState: ListState[UserLoginEvent] = _

        override def processElement(value: UserLoginEvent, ctx: KeyedProcessFunction[Long, UserLoginEvent, UserLoginEvent]#Context, out: Collector[UserLoginEvent]): Unit = {
            loginFailState.add(value)
            ctx.timerService().registerEventTimeTimer(value.timestamp + 2 * 1000)

            // 输出
            out.collect(value)
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UserLoginEvent, UserLoginEvent]#OnTimerContext,
                             out: Collector[UserLoginEvent]): Unit = {
            // 获取
            val loginEvents: ListBuffer[UserLoginEvent] = ListBuffer()
            loginFailState.get().forEach(value => loginEvents.append(value))
            // 如果大于两次就输出警告
            if (loginEvents.size > times) {
                ctx.output(outputTag, s"用户: ${ctx.getCurrentKey} 在 2S 内登录失败${times}次，请处理")
            }
            // 清空
            loginFailState.clear()
        }

        override def open(parameters: Configuration): Unit = {
            loginFailState = getRuntimeContext.getListState(new ListStateDescriptor[UserLoginEvent]("login-fail-state", classOf[UserLoginEvent]))
        }
    }
}
