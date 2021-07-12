package com.hadoop.study.fraud.detect.beans

import com.hadoop.study.fraud.detect.enums.AggregateType.Aggregate
import com.hadoop.study.fraud.detect.enums.ControlType.Control
import com.hadoop.study.fraud.detect.enums.OperateType._
import com.hadoop.study.fraud.detect.enums.RuleState.State
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

    var ruleState: State = _

    var groupingKeyNames: List[String] = List()

    var aggregateFieldName: String = _

    var aggregatorType: Aggregate = _

    var limitOperatorType: Operate = _

    var limit: BigDecimal = _

    var windowMinutes: Int = 0

    var controlType: Control = _

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

    def getWindowMillis: Long = Time.minutes(this.windowMinutes).toMilliseconds
}
