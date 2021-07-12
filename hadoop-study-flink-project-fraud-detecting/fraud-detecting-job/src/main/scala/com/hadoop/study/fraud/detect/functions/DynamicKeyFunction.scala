package com.hadoop.study.fraud.detect.functions

import com.hadoop.study.fraud.detect.beans._
import com.hadoop.study.fraud.detect.dynamic.Descriptors
import com.hadoop.study.fraud.detect.enums.ControlType.ControlType
import com.hadoop.study.fraud.detect.enums.{ControlType, RuleState}
import com.hadoop.study.fraud.detect.utils.KeysExtractor
import com.hadoop.study.fraud.detect.utils.StateUtils.handleBroadcast
import org.apache.flink.api.common.state.{BroadcastState, ReadOnlyBroadcastState}
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 16:29
 */

case class DynamicKeyFunction() extends BroadcastProcessFunction[Transaction, Rule, Keyed[Transaction, String, Int]] {

    private val log = LoggerFactory.getLogger(classOf[DynamicKeyFunction])

    private var ruleCounterGauge = 0

    override def open(parameters: Configuration): Unit = {
        getRuntimeContext.getMetricGroup.gauge[Int, ScalaGauge[Int]]("numberOfActiveRules", ScalaGauge[Int](() => ruleCounterGauge))
    }

    override def processElement(event: Transaction, ctx: BroadcastProcessFunction[Transaction, Rule, Keyed[Transaction, String, Int]]#ReadOnlyContext, out: Collector[Keyed[Transaction, String, Int]]): Unit = {
        val rulesState = ctx.getBroadcastState(Descriptors.rulesDescriptor)
        forkEventForEachGroupingKey(event, rulesState, out)
    }

    private def forkEventForEachGroupingKey(event: Transaction, rulesState: ReadOnlyBroadcastState[Int, Rule], out: Collector[Keyed[Transaction, String, Int]]): Unit = {
        var ruleCounter = 0

        rulesState.immutableEntries.forEach(entry => {
            val rule = entry.getValue
            out.collect(Keyed(event, KeysExtractor.getKey(rule.groupingKeyNames, event), rule.ruleId))
            ruleCounter += 1
        })

        ruleCounterGauge += ruleCounter
    }

    override def processBroadcastElement(value: Rule, ctx: BroadcastProcessFunction[Transaction, Rule, Keyed[Transaction, String, Int]]#Context, out: Collector[Keyed[Transaction, String, Int]]): Unit = {
        log.trace(s"processBroadcastElement ${value}")
        val broadcastState = ctx.getBroadcastState(Descriptors.rulesDescriptor)
        handleBroadcast(value, broadcastState)
        if (value.ruleState eq RuleState.CONTROL)
            handleControlCommand(value.controlType, broadcastState)
    }

    private def handleControlCommand(controlType: ControlType, rulesState: BroadcastState[Int, Rule]): Unit = {
        if (controlType eq ControlType.DELETE_RULES_ALL) {
            val iter = rulesState.iterator
            while (iter.hasNext) {
                val ruleEntry = iter.next
                rulesState.remove(ruleEntry.getKey)
                log.trace(s"Removed ${ruleEntry.getValue}")
            }
        }
    }
}