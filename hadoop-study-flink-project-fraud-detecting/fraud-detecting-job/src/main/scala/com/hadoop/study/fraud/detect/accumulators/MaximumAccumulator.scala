package com.hadoop.study.fraud.detect.accumulators

import org.apache.flink.api.common.accumulators.{Accumulator, SimpleAccumulator}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 11:48
 */

case class MaximumAccumulator(var max: BigDecimal = BigDecimal.decimal(Double.MinValue), limit: BigDecimal = BigDecimal.decimal(Double.MinValue)) extends SimpleAccumulator[BigDecimal] {

    override def add(value: BigDecimal): Unit = {
        if (value.compareTo(limit) < 0)
            throw new IllegalArgumentException("BigDecimalMaximum accumulator only supports values greater than Double.MIN_VALUE")
        max = max.max(value)
    }

    override def getLocalValue: BigDecimal = max

    override def resetLocal(): Unit = max = BigDecimal.decimal(Double.MinValue)

    override def merge(other: Accumulator[BigDecimal, BigDecimal]): Unit = max = max.max(other.getLocalValue)

    override def clone(): MaximumAccumulator = MaximumAccumulator(max)

    override def toString: String = s"BigDecimal Maximum ${max}"
}
