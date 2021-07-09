package com.hadoop.study.fraud.detect.beans

import com.hadoop.study.fraud.detect.beans.AggregatorType.AggregatorType
import com.hadoop.study.fraud.detect.beans.ControlType.ControlType
import com.hadoop.study.fraud.detect.beans.OperatorType.{EQUAL, GREATER, GREATER_EQUAL, LESS, LESS_EQUAL, NOT_EQUAL, OperatorType}
import com.hadoop.study.fraud.detect.beans.RuleState.RuleState
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 11:10
 */
class Rule {

    var ruleId: Int = 0

    var ruleState: RuleState = _

    var groupingKeyNames: List[String] = List()

    var aggregateFieldName: String = _

    var aggregatorType: AggregatorType = _

    var limitOperatorType: OperatorType = _

    var limit: BigDecimal = _

    var windowMinutes: Int = 0

    var controlType: ControlType = _

    def getWindowMillis: Long = Time.minutes(this.windowMinutes).toMilliseconds

    def apply(comparisonValue: BigDecimal): Boolean =
        limitOperatorType match {
            case EQUAL =>
                comparisonValue.compareTo(limit) == 0
            case NOT_EQUAL =>
                comparisonValue.compareTo(limit) != 0
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

object AggregatorType extends Enumeration {
    type AggregatorType = Value

    val SUM, AVG, MIN, MAX = Value
}

object OperatorType extends Enumeration {
    type OperatorType = Value

    val EQUAL, NOT_EQUAL, GREATER_EQUAL, LESS_EQUAL, GREATER, LESS = Value
}

object RuleState extends Enumeration {
    type RuleState = Value

    val ACTIVE, PAUSE, DELETE, CONTROL = Value
}

object ControlType extends Enumeration {
    type ControlType = Value

    val CLEAR_STATE_ALL, DELETE_RULES_ALL, EXPORT_RULES_CURRENT = Value
}