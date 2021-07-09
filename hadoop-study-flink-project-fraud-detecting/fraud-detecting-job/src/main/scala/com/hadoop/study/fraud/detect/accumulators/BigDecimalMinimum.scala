package com.hadoop.study.fraud.detect.accumulators

import org.apache.flink.api.common.accumulators.{Accumulator, SimpleAccumulator}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 11:54
 */

case class BigDecimalMinimum(var min: BigDecimal = BigDecimal.decimal(Double.MaxValue), limit: BigDecimal = BigDecimal.decimal(Double.MaxValue)) extends SimpleAccumulator[BigDecimal] {

    override def add(value: BigDecimal): Unit = {
        if (value.compareTo(limit) > 0) throw new IllegalArgumentException("BigDecimalMinimum accumulator only supports values less than Double.MAX_VALUE")
        this.min = min.min(value)
    }

    override def getLocalValue: BigDecimal = min

    override def resetLocal(): Unit = min = BigDecimal.decimal(Double.MaxValue)

    override def merge(other: Accumulator[BigDecimal, BigDecimal]): Unit = min = min.min(other.getLocalValue)

    override def clone(): BigDecimalMinimum = BigDecimalMinimum(min)

    override def toString: String = s"BigDecimal ${min}"
}

