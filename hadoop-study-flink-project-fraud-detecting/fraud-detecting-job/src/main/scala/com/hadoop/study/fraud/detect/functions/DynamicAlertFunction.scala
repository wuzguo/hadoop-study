package com.hadoop.study.fraud.detect.functions

import com.hadoop.study.fraud.detect.beans.ControlType._
import com.hadoop.study.fraud.detect.beans.{AlertEvent, Keyed, Rule, RuleState}
import com.hadoop.study.fraud.detect.dynamic.{Descriptors, FieldsExtractor, RuleHelper, Transaction}
import com.hadoop.study.fraud.detect.functions.ProcessingUtils.handleRuleBroadcast
import org.apache.flink.api.common.accumulators.SimpleAccumulator
import org.apache.flink.api.common.state.{BroadcastState, MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.{Meter, MeterView}
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 13:48
 */

class DynamicAlertFunction extends KeyedBroadcastProcessFunction[String, Keyed[Transaction, String, Int], Rule, AlertEvent[Transaction, BigDecimal]] {

    private val log = LoggerFactory.getLogger(classOf[DynamicAlertFunction])

    private val COUNT = "COUNT_FLINK"

    private val COUNT_WITH_RESET = "COUNT_WITH_RESET_FLINK"

    private val WIDEST_RULE_KEY = Integer.MIN_VALUE

    private var windowState: MapState[Long, mutable.Set[Transaction]] = _

    private var alertMeter: Meter = _

    private val windowStateDescriptor = new MapStateDescriptor[Long, Set[Transaction]]("windowState", classOf[Long], classOf[Set[Transaction]])

    override def processElement(value: Keyed[Transaction, String, Int], ctx: KeyedBroadcastProcessFunction[String, Keyed[Transaction, String, Int], Rule, AlertEvent[Transaction, BigDecimal]]#ReadOnlyContext, out: Collector[AlertEvent[Transaction, BigDecimal]]): Unit = {
        log.trace("processElement value: {}, alert: {}", value)
        val currentEventTime = value.wrapped.eventTime
        ProcessingUtils.addToStateValuesSet(windowState, currentEventTime, value.wrapped)

        val ingestionTime = value.wrapped.ingestionTimestamp
        ctx.output(Descriptors.latencySinkTag, System.currentTimeMillis - ingestionTime)

        val rule = ctx.getBroadcastState(Descriptors.rulesDescriptor).get(value.id)

        // This could happen if the BroadcastState in this CoProcessFunction was updated after it was
        if (rule == null) {
            // updated and used in `DynamicKeyFunction`
            // TODO: you may want to handle this situation differently, e.g. by versioning rules and
            //       handling them by the same version throughout the whole pipeline, or by buffering
            //       events waiting for rules to come through
            return
        }

        if (rule.ruleState eq RuleState.ACTIVE) {
            val windowStartForEvent = rule.getWindowStartFor(currentEventTime)
            val cleanupTime = (currentEventTime / 1000) * 1000
            ctx.timerService.registerEventTimeTimer(cleanupTime)
            val aggregator = RuleHelper.getAggregator(rule)

            windowState.keys.forEach(stateEventTime => {
                if (isStateValueInWindow(stateEventTime, windowStartForEvent, currentEventTime))
                    aggregateValuesInState(stateEventTime, aggregator, rule)
            })

            val aggregateResult = aggregator.getLocalValue
            val ruleResult = rule.apply(aggregateResult)
            log.trace("Rule {} | {} : {} -> {}", rule.ruleId, value.key, aggregateResult, ruleResult)
            if (ruleResult) {
                if (COUNT_WITH_RESET.equals(rule.aggregateFieldName))
                    evictAllStateElements()
                alertMeter.markEvent()
                out.collect(AlertEvent(rule.ruleId, rule, value.key, value.wrapped, aggregateResult))
            }
        }
    }

    override def processBroadcastElement(value: Rule, ctx: KeyedBroadcastProcessFunction[String, Keyed[Transaction, String, Int], Rule, AlertEvent[Transaction, BigDecimal]]#Context, out: Collector[AlertEvent[Transaction, BigDecimal]]): Unit = {
        log.trace("processBroadcastElement {}", value)
        val broadcastState = ctx.getBroadcastState(Descriptors.rulesDescriptor)
        handleRuleBroadcast(value, broadcastState)
        updateWidestWindowRule(value, broadcastState)
        if (value.ruleState eq RuleState.CONTROL)
            handleControlCommand(value, broadcastState, ctx)
    }

    override def open(parameters: Configuration): Unit = {
        windowState = getRuntimeContext.getMapState(windowStateDescriptor)
        alertMeter = new MeterView(60)
        getRuntimeContext.getMetricGroup.meter("alertsPerSecond", alertMeter)
    }

    @throws[Exception]
    private def handleControlCommand(command: Rule, rulesState: BroadcastState[Int, Rule], ctx: KeyedBroadcastProcessFunction[String, Keyed[Transaction,
      String, Int], Rule, AlertEvent[Transaction, BigDecimal]]#Context): Unit = {
        command.controlType match {
            case EXPORT_RULES_CURRENT =>
                rulesState.entries.forEach(entry => ctx.output(Descriptors.currentRulesSinkTag, entry.getValue))
            case CLEAR_STATE_ALL =>
                ctx.applyToKeyedState(windowStateDescriptor, (_, state) => state.clear())
            case DELETE_RULES_ALL =>
                val entriesIterator = rulesState.iterator
                while (entriesIterator.hasNext) {
                    val ruleEntry = entriesIterator.next
                    rulesState.remove(ruleEntry.getKey)
                    log.trace("Removed {}", ruleEntry.getValue)
                }
        }
    }

    private def isStateValueInWindow(stateEventTime: Long, windowStartForEvent: Long, currentEventTime: Long) = stateEventTime >= windowStartForEvent && stateEventTime <= currentEventTime

    @throws[Exception]
    private def aggregateValuesInState(stateEventTime: Long, aggregator: SimpleAccumulator[BigDecimal], rule: Rule): Unit = {
        val inWindow = windowState.get(stateEventTime)
        if (COUNT.equals(rule.aggregateFieldName) || COUNT_WITH_RESET.equals(rule.aggregateFieldName)) {
            for (_ <- inWindow) {
                aggregator.add(BigDecimal(1))
            }
        } else {
            for (event <- inWindow) {
                val aggregatedValue = FieldsExtractor.getBigDecimalByName(rule.aggregateFieldName, event)
                aggregator.add(aggregatedValue)
            }
        }
    }

    @throws[Exception]
    private def updateWidestWindowRule(rule: Rule, broadcastState: BroadcastState[Int, Rule]): Unit = {
        val widestWindowRule = broadcastState.get(WIDEST_RULE_KEY)
        if (widestWindowRule != null && (widestWindowRule.ruleState eq RuleState.ACTIVE))
            if (widestWindowRule.getWindowMillis < rule.getWindowMillis)
                broadcastState.put(WIDEST_RULE_KEY, rule)
    }

    override def onTimer(timestamp: Long, ctx: KeyedBroadcastProcessFunction[String, Keyed[Transaction, String, Int], Rule, AlertEvent[Transaction, BigDecimal]]#OnTimerContext, out: Collector[AlertEvent[Transaction, BigDecimal]]): Unit = {
        val widestWindowRule = ctx.getBroadcastState(Descriptors.rulesDescriptor).get(WIDEST_RULE_KEY)
        if (widestWindowRule != null) {
            val cleanupEventTimeThreshold = timestamp - widestWindowRule.getWindowMillis
            if (cleanupEventTimeThreshold > 0L) {
                this.evictAgedElementsFromWindow(cleanupEventTimeThreshold)
            }
        }
    }

    private def evictAgedElementsFromWindow(threshold: Long): Unit = {
        try {
            val keys = windowState.keys.iterator
            while (keys.hasNext) {
                val stateEventTime = keys.next
                if (stateEventTime < threshold) keys.remove()
            }
        } catch {
            case ex: Exception => throw new RuntimeException(ex)
        }
    }

    private def evictAllStateElements(): Unit = {
        try {
            val keys = windowState.keys.iterator
            while (keys.hasNext) {
                keys.next
                keys.remove()
            }
        } catch {
            case ex: Exception => throw new RuntimeException(ex)
        }
    }
}
