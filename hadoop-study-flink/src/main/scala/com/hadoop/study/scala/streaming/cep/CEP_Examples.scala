package com.hadoop.study.scala.streaming.cep

import com.hadoop.study.scala.example.beans.UserBehavior
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time

import java.sql.Timestamp
import java.util
import scala.collection.mutable.ListBuffer

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/24 14:43
 */

object CEP_Examples {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 读取数据
        val inputStream = env.readTextFile("./hadoop-study-datas/flink/input/UserBehavior.csv")

        // 行为数据，加上WaterMarker的数据，由于数据是ETL数据，已经排好序
        val dataStream = inputStream.map(line => {
            val values = line.split(",")
            UserBehavior(values(0).toLong, values(1).toLong, values(2).toInt, values(3), new Timestamp(values(4).toLong * 1000))
        }).assignAscendingTimestamps(_.timestamp.getTime)

        val userPattern = Pattern.begin[UserBehavior]("pv-pattern")
          .where(_.action == "pv")
          .times(3)
          .consecutive() // 连续的
          .within(Time.seconds(3))

        // 2. 将模式应用到数据流上，得到一个PatternStream
        val patternStream = CEP.pattern(dataStream.keyBy(_.userId), userPattern)

        // 3. 打印
        val resultStream = patternStream.select(new CustomPatternFunction)
        resultStream.print()

        // 4. 执行
        env.execute("CEP Examples")
    }

    class CustomPatternFunction extends PatternSelectFunction[UserBehavior, String] {

        override def select(pattern: util.Map[String, util.List[UserBehavior]]): String = {
            // 这些数据
            val patternKey = pattern.keySet().iterator().next()
            val itemIds: ListBuffer[Long] = ListBuffer()
            val behaviors = pattern.get(patternKey)
            behaviors.forEach(behavior => itemIds.append(behavior.itemId))
            s"Pattern: ${patternKey}，用户：${behaviors.iterator().next().userId}, 匹配列表：${itemIds.mkString(",")}"
        }
    }
}
