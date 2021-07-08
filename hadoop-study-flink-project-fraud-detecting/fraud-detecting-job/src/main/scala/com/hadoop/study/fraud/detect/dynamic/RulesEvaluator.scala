package com.hadoop.study.fraud.detect.dynamic

import com.hadoop.study.fraud.detect.beans.Rule
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.kafka.common.config.Config

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 9:27
 */

case class RulesEvaluator(config: Config) {

}

object RulesEvaluator {


}


object Descriptors {
    val rulesDescriptor = new MapStateDescriptor[Int, Rule]("rules", classOf[Int], classOf[Rule])

    val latencySinkTag: OutputTag[Long] = new OutputTag[Long]("latency-sink")

    val currentRulesSinkTag: OutputTag[Rule] = new OutputTag[Rule]("current-rules-sink")
}