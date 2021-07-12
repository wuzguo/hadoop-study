package com.hadoop.study.fraud.detect.utils

import com.hadoop.study.fraud.detect.accumulators.{AverageAccumulator, BigDecimalCounter, BigDecimalMaximum, BigDecimalMinimum}
import com.hadoop.study.fraud.detect.beans.Rule
import com.hadoop.study.fraud.detect.enums.AggregatorType.{AVG, MAX, MIN, SUM}
import org.apache.flink.api.common.accumulators.SimpleAccumulator

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 16:21
 */

object RuleHelper {

    /* Picks and returns a new accumulator, based on the Rule's aggregator function type. */
    def getAggregator(rule: Rule): SimpleAccumulator[BigDecimal] =
        rule.aggregatorType match {
            case SUM =>
                BigDecimalCounter()
            case AVG =>
                AverageAccumulator(0, BigDecimal(0))
            case MAX =>
                BigDecimalMaximum()
            case MIN =>
                BigDecimalMinimum()
            case _ =>
                throw new RuntimeException(s"Unsupported aggregation function type: ${rule.aggregatorType}")
        }

}
