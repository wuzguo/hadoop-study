package com.hadoop.study.fraud.detect.beans

import com.hadoop.study.fraud.detect.dynamic.JsonMapper2
import com.hadoop.study.fraud.detect.enums.OperateType
import com.hadoop.study.fraud.detect.enums.OperateType._
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

    var ruleState: String = _

    var groupingKeyNames: List[String] = List()

    var aggregateFieldName: String = _

    var aggregatorType: String = _

    var operatorType: String = _

    var limit: BigDecimal = _

    var windowMinutes: Int = 0

    var controlType: String = _

    def apply(value: BigDecimal): Boolean = OperateType.withName(operatorType) match {
        case EQUAL =>
            value.compareTo(limit) == 0
        case NOT_EQUAL =>
            value.compareTo(limit) != 0
        case GREATER =>
            value.compareTo(limit) > 0
        case LESS =>
            value.compareTo(limit) < 0
        case LESS_EQUAL =>
            value.compareTo(limit) <= 0
        case GREATER_EQUAL =>
            value.compareTo(limit) >= 0
        case _ =>
            throw new RuntimeException("unknown limit operator type: " + operatorType)
    }

    def getWindowStartFor(timestamp: Long): Long = {
        val ruleWindowMillis = getWindowMillis
        timestamp - ruleWindowMillis
    }

    def getWindowMillis: Long = Time.minutes(this.windowMinutes).toMilliseconds

    override def toString: String = JsonMapper2(classOf[Rule]).to(this)
}
