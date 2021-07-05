package com.hadoop.study.scala.example.login

import com.hadoop.study.scala.example.beans.UserLoginEvent
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/21 19:15
 */

object LoginDetectAdvanceAnalysis {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream = env.readTextFile("./hadoop-study-datas/flink/input/LoginLog.csv")

        val dataStream = inputStream.map(line => {
            val values = line.split(",")
            UserLoginEvent(values(0).toLong, values(1), values(2), values(3).toLong * 1000)
        }).assignTimestampsAndWatermarks(
            WatermarkStrategy.forBoundedOutOfOrderness[UserLoginEvent](Duration.ofSeconds(2))
              .withTimestampAssigner(new SerializableTimestampAssigner[UserLoginEvent] {
                  override def extractTimestamp(element: UserLoginEvent, recordTimestamp: Long): Long = element.timestamp
              }))

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
            // 1. 获取列表
            val iter = loginFailState.get().iterator()
            // 判断之前是否有登录失败事件
            if (iter.hasNext) {
                // 1.1 如果有，那么判断两次失败的时间差
                val lastFailEvent = iter.next()
                if (value.timestamp < (lastFailEvent.timestamp + 2000)) {
                    // 如果在2秒之内，输出报警
                    ctx.output(outputTag, s"用户: ${ctx.getCurrentKey} 在 2S 内登录失败${times}次，请处理")
                }
                // 不管报不报警，当前都已处理完毕，将状态更新为最近依次登录失败的事件
                loginFailState.clear()
            }

            // 1.2 不管有没有，直接把当前事件添加到ListState中
            loginFailState.add(value)
            // 输出
            out.collect(value)
        }

        override def open(parameters: Configuration): Unit = {
            loginFailState = getRuntimeContext.getListState(new ListStateDescriptor[UserLoginEvent]("login-fail-state", classOf[UserLoginEvent]))
        }
    }
}
