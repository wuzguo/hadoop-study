package com.hadoop.study.fraud.detect.beans

import com.hadoop.study.fraud.detect.beans.LimitOperatorType._
import com.hadoop.study.fraud.detect.beans.RuleState.Type
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 11:10
 */

case class Rule(ruleId: Int, ruleState: Type, groupingKeyNames: List[String], aggregateFieldName: String,
                aggregatorFunctionType: Type, limitOperatorType: Type,
                limit: BigDecimal, windowMinutes: Int) {

    var controlType: Type = _

    def getWindowMillis: Long = Time.minutes(this.windowMinutes).toMilliseconds

    def apply(comparisonValue: BigDecimal): Boolean =
        limitOperatorType match {
            case EQUAL =>
                comparisonValue.compareTo(limit) eq 0
            case NOT_EQUAL =>
                comparisonValue.compareTo(limit) ne 0
            case GREATER =>
                comparisonValue.compareTo(limit) > 0
            case LESS =>
                comparisonValue.compareTo(limit) < 0
            case LESS_EQUAL =>
                comparisonValue.compareTo(limit) <= 0
            case GREATER_EQUAL =>
                comparisonValue.compareTo(limit) >= 0
            case _ =>
                throw new RuntimeException("Unknown limit operator type: " + limitOperatorType)
        }

    def getWindowStartFor(timestamp: Long): Long = {
        val ruleWindowMillis = getWindowMillis
        timestamp - ruleWindowMillis
    }
}


object AggregatorFunctionType extends Enumeration {
    type Type = Value

    val SUM, AVG, MIN, MAX = Value
}

object LimitOperatorType extends Enumeration {
    type Type = Value

    val EQUAL, NOT_EQUAL, GREATER_EQUAL, LESS_EQUAL, GREATER, LESS = Value
}

object RuleState extends Enumeration {
    type Type = Value

    val ACTIVE, PAUSE, DELETE, CONTROL = Value
}

object ControlType extends Enumeration {
    type Type = Value

    val CLEAR_STATE_ALL, DELETE_RULES_ALL, EXPORT_RULES_CURRENT = Value
}