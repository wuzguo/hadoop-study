package com.hadoop.study.fraud.detect.functions

import com.hadoop.study.fraud.detect.beans.ControlType.ControlType
import com.hadoop.study.fraud.detect.beans.{ControlType, Keyed, Rule}
import com.hadoop.study.fraud.detect.dynamic.{Descriptors, KeysExtractor, Transaction}
import org.apache.flink.api.common.state.{BroadcastState, ReadOnlyBroadcastState}
import org.apache.flink.metrics.Gauge
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

    private var ruleCounterGauge: RuleCounterGauge = _

    def open(parameters: Nothing): Unit = {
        ruleCounterGauge = new RuleCounterGauge
        getRuntimeContext.getMetricGroup.gauge("numberOfActiveRules", ruleCounterGauge)
    }

    override def processElement(event: Transaction, ctx: BroadcastProcessFunction[Transaction, Rule,
      Keyed[Transaction, String, Int]]#ReadOnlyContext, out: Collector[Keyed[Transaction, String, Int]]): Unit = {

        val rulesState = ctx.getBroadcastState(Descriptors.rulesDescriptor)
        forkEventForEachGroupingKey(event, rulesState, out)
    }

    override def processBroadcastElement(value: Rule, ctx: BroadcastProcessFunction[Transaction, Rule,
      Keyed[Transaction, String, Int]]#Context, out: Collector[Keyed[Transaction, String, Int]]): Unit = {

    }

    private def forkEventForEachGroupingKey(event: Transaction, rulesState: ReadOnlyBroadcastState[Int, Rule], out: Collector[Keyed[Transaction, String, Int]]): Unit = {
        var ruleCounter = 0

        rulesState.immutableEntries.forEach(entry => {
            val rule = entry.getValue
            out.collect(Keyed(event, KeysExtractor.getKey(rule.groupingKeyNames, event), rule.ruleId))
            ruleCounter += 1
        })

        ruleCounterGauge.setValue(ruleCounter)
    }


    private def handleControlCommand(controlType: ControlType, rulesState: BroadcastState[Integer, Nothing]): Unit = {
        if (controlType eq ControlType.DELETE_RULES_ALL) {
            val entriesIterator = rulesState.iterator
            while (entriesIterator.hasNext) {
                val ruleEntry = entriesIterator.next
                rulesState.remove(ruleEntry.getKey)
                log.trace("Removed {}", ruleEntry.getValue)
            }
        }
    }
}


private class RuleCounterGauge extends Gauge[String] {
    private var value = 0

    def setValue(value: Int): Unit = {
        this.value = value
    }

    def getValue: Integer = value
}