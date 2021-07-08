package com.hadoop.study.fraud.detect.accumulators

import org.apache.flink.api.common.accumulators.{Accumulator, SimpleAccumulator}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 11:42
 */

case class BigDecimalCounter(var local: BigDecimal = BigDecimal(0)) extends SimpleAccumulator[BigDecimal] {

    override def add(value: BigDecimal): Unit = local += value

    override def getLocalValue: BigDecimal = local

    override def resetLocal(): Unit = local = BigDecimal(0)

    override def merge(other: Accumulator[BigDecimal, BigDecimal]): Unit = local += other.getLocalValue

    override def clone(): AnyRef = BigDecimalCounter(local)

    override def toString: String = s"BigDecimalCounter ${local}"
}
