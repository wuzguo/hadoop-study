package com.hadoop.study.scala.example.login

import com.hadoop.study.scala.example.beans.UserLoginEvent
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter

import java.util

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/21 19:15
 */

object LoginDetectWithCEPAnalysis {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream = env.readTextFile("./hadoop-study-datas/flink/input/LoginLog.csv")

        val dataStream = inputStream.map(line => {
            val values = line.split(",")
            UserLoginEvent(values(0).toLong, values(1), values(2), values(3).toLong * 1000)
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy(new TimestampExtractor(Time.seconds(2))))

        val loginFailPattern = Pattern.begin[UserLoginEvent]("login-fail").where(_.result == "fail").times(3)
          .consecutive()
          .within(Time.seconds(5))

        // 2. 将模式应用到数据流上，得到一个PatternStream
        val patternStream = CEP.pattern(dataStream.keyBy(_.userId), loginFailPattern)

        // 3. 检出符合模式的数据流，需要调用select
        val failWarningStream = patternStream.select(new LoginFailPatternFunction)

        failWarningStream.print()

        // 执行
        env.execute("Login Fail Detect With CEP Analysis")
    }


    // 自定义 timestamp extractor
    class TimestampExtractor(maxOutOfOrderness: Time) extends BoundedOutOfOrdernessTimestampExtractor[UserLoginEvent](maxOutOfOrderness: Time) {

        override def extractTimestamp(element: UserLoginEvent): Long = element.timestamp
    }


    // 实现自定义PatternSelectFunction
    class LoginFailPatternFunction extends PatternSelectFunction[UserLoginEvent, String] {

        override def select(map: util.Map[String, util.List[UserLoginEvent]]): String = {

            val iter = map.get("login-fail").iterator()
            val firstFailEvent = iter.next()

            s"用户: ${firstFailEvent.userId} 在 2S 内登录失败2次，请处理"
        }
    }

}
