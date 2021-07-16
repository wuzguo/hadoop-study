package com.hadoop.study.fraud.detect.functions

import com.hadoop.study.fraud.detect.beans._
import com.hadoop.study.fraud.detect.dynamic.{Descriptors, Tags}
import com.hadoop.study.fraud.detect.enums.ControlType.{CLEAR_STATE_ALL, DELETE_RULES_ALL, EXPORT_RULES_CURRENT}
import com.hadoop.study.fraud.detect.enums.{ControlType, RuleState}
import com.hadoop.study.fraud.detect.utils.StateUtils.handleBroadcast
import com.hadoop.study.fraud.detect.utils.{FieldsExtractor, RuleHelper, StateUtils}
import org.apache.flink.api.common.accumulators.SimpleAccumulator
import org.apache.flink.api.common.state.{BroadcastState, MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.{Meter, MeterView}
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.reflect.runtime.universe.typeOf

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 13:48
 */

case class DynamicAlertFunction() extends KeyedBroadcastProcessFunction[String, Keyed[Transaction, String, Int], Rule, AlertEvent[Transaction, BigDecimal]] {

    private val log = LoggerFactory.getLogger(classOf[DynamicAlertFunction])

    private val COUNT = "COUNT_FLINK"

    private val COUNT_WITH_RESET = "COUNT_WITH_RESET_FLINK"

    private val WIDEST_RULE_KEY = Int.MinValue

    private val windowStateDescriptor = new MapStateDescriptor[Long, Set[Transaction]]("windowState", classOf[Long], classOf[Set[Transaction]])

    private var windowState: MapState[Long, Set[Transaction]] = _

    private var alertMeter: Meter = _

    override def processElement(value: Keyed[Transaction, String, Int], ctx: KeyedBroadcastProcessFunction[String, Keyed[Transaction, String, Int], Rule, AlertEvent[Transaction, BigDecimal]]#ReadOnlyContext, out: Collector[AlertEvent[Transaction, BigDecimal]]): Unit = {
        log.info("processElement value: {}", value)
        // 交易的eventTime
        val eventTime = value.wrapped.eventTime
        StateUtils.addValues(windowState, eventTime, value.wrapped)

        // 进入Fink时间
        val ingestionTime = value.wrapped.ingestionTimestamp
        // 延迟时间
        ctx.output(Tags.latencySinkTag, System.currentTimeMillis - ingestionTime)

        val rule = ctx.getBroadcastState(Descriptors.rulesDescriptor).get(value.id)
        // This could happen if the BroadcastState in this CoProcessFunction was updated after it was
        if (rule == null) {
            log.info("rule get null, rule id: {}", value.id)
            return
        }

        // 如果规则是激活状态
        if (RuleState.withName(rule.ruleState) eq RuleState.ACTIVE) {
            val startWindowTime = rule.getWindowStartFor(eventTime)
            // 秒数取整
            val cleanupTime = (eventTime / 1000) * 1000
            // 注册定时器
            ctx.timerService.registerEventTimeTimer(cleanupTime)
            // 获取聚合函数
            val aggregator = RuleHelper.getAggregator(rule)

            windowState.keys.forEach(stateEventTime => {
                if (isStateValueInWindow(stateEventTime, startWindowTime, eventTime))
                    aggregateValuesInState(stateEventTime, aggregator, rule)
            })

            // 获取结果
            val result = aggregator.getLocalValue
            // 计算是不是满足规则要求
            val ruleResult = rule.apply(result)
            log.trace(s"rule ${rule.ruleId} | ${value.key} : ${result} -> ${ruleResult}")

            // 如果结果为真就输出
            if (ruleResult) {
                // 如果需要重置
                if (COUNT_WITH_RESET.equals(rule.aggregateFieldName))
                    evictAllStateElements()

                alertMeter.markEvent()
                out.collect(AlertEvent(rule.ruleId, rule, value.key, value.wrapped, result))
            }
        }
    }

    private def isStateValueInWindow(stateEventTime: Long, startWindowTime: Long, eventTime: Long) = stateEventTime >= startWindowTime && stateEventTime <= eventTime

    private def aggregateValuesInState(stateEventTime: Long, aggregator: SimpleAccumulator[BigDecimal], rule: Rule): Unit = {
        val transactions = windowState.get(stateEventTime)
        // 如果是计数
        if (COUNT.equals(rule.aggregateFieldName) || COUNT_WITH_RESET.equals(rule.aggregateFieldName)) {
            for (_ <- transactions) {
                aggregator.add(BigDecimal(1))
            }
        } else {
            for (event <- transactions) {
                // 反射获取指定列的值，其实前端只支持 paymentAmount
                val aggregatedValue = FieldsExtractor.getBigDecimalByName(event, typeOf[Transaction], rule.aggregateFieldName)
                aggregator.add(aggregatedValue)
            }
        }
    }

    private def evictAllStateElements(): Unit = {
        val keys = windowState.keys.iterator
        while (keys.hasNext) {
            keys.next
            keys.remove()
        }
    }

    override def processBroadcastElement(value: Rule, ctx: KeyedBroadcastProcessFunction[String, Keyed[Transaction, String, Int], Rule, AlertEvent[Transaction, BigDecimal]]#Context, out: Collector[AlertEvent[Transaction, BigDecimal]]): Unit = {
        log.info(s"processBroadcastElement ${value}")
        val broadcastState = ctx.getBroadcastState(Descriptors.rulesDescriptor)
        handleBroadcast(value, broadcastState)
        updateWidestWindowRule(value, broadcastState)
        if (RuleState.withName(value.ruleState) eq RuleState.CONTROL)
            handleControlCommand(value, broadcastState, ctx)
    }

    private def handleControlCommand(command: Rule, rulesState: BroadcastState[Int, Rule], ctx: KeyedBroadcastProcessFunction[String, Keyed[Transaction, String, Int], Rule, AlertEvent[Transaction, BigDecimal]]#Context): Unit = {
        ControlType.withName(command.controlType) match {
            case EXPORT_RULES_CURRENT =>
                rulesState.entries.forEach(entry => ctx.output(Tags.currentRulesSinkTag, entry.getValue))
            case CLEAR_STATE_ALL =>
                ctx.applyToKeyedState(windowStateDescriptor, (_, state: MapState[Long, Set[Transaction]]) => state.clear())
            case DELETE_RULES_ALL =>
                val entriesIterator = rulesState.iterator
                while (entriesIterator.hasNext) {
                    val ruleEntry = entriesIterator.next
                    rulesState.remove(ruleEntry.getKey)
                    log.trace(s"removed ${ruleEntry.getValue}")
                }
            case _ =>
        }
    }

    private def updateWidestWindowRule(rule: Rule, broadcastState: BroadcastState[Int, Rule]): Unit = {
        val widestWindowRule = broadcastState.get(WIDEST_RULE_KEY)
        if (widestWindowRule != null && (RuleState.withName(widestWindowRule.ruleState) eq RuleState.ACTIVE))
            if (widestWindowRule.getWindowMillis < rule.getWindowMillis)
                broadcastState.put(WIDEST_RULE_KEY, rule)
    }

    override def open(parameters: Configuration): Unit = {
        windowState = getRuntimeContext.getMapState(windowStateDescriptor)
        alertMeter = new MeterView(60)
        getRuntimeContext.getMetricGroup.meter("alertsPerSecond", alertMeter)
    }

    override def onTimer(timestamp: Long, ctx: KeyedBroadcastProcessFunction[String, Keyed[Transaction, String, Int], Rule, AlertEvent[Transaction, BigDecimal]]#OnTimerContext, out: Collector[AlertEvent[Transaction, BigDecimal]]): Unit = {
        val widestWindowRule = ctx.getBroadcastState(Descriptors.rulesDescriptor).get(WIDEST_RULE_KEY)
        // 每隔一秒钟清空已过时的数据，避免OOM
        if (widestWindowRule != null) {
            val cleanupEventTimeThreshold = timestamp - widestWindowRule.getWindowMillis
            if (cleanupEventTimeThreshold > 0L) {
                this.evictAgedElementsFromWindow(cleanupEventTimeThreshold)
            }
        }
    }

    private def evictAgedElementsFromWindow(threshold: Long): Unit = {
        val keys = windowState.keys.iterator
        while (keys.hasNext) {
            val stateEventTime = keys.next
            if (stateEventTime < threshold) keys.remove()
        }
    }
}
