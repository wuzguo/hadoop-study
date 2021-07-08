package com.hadoop.study.fraud.detect.accumulators

import org.apache.flink.api.common.accumulators.{Accumulator, SimpleAccumulator}

import java.math.MathContext

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 11:22
 */

case class AverageAccumulator(var count: Long, var sum: BigDecimal) extends SimpleAccumulator[BigDecimal] {

    override def add(value: BigDecimal): Unit = {
        count += 1
        sum += value
    }

    override def getLocalValue: BigDecimal = {
        if (count eq 0L) return BigDecimal(0)

        this.sum / BigDecimal(count) round MathContext.UNLIMITED
    }

    override def resetLocal(): Unit = {
        count = 0
        sum = BigDecimal(0)
    }

    override def merge(other: Accumulator[BigDecimal, BigDecimal]): Unit = {
        other match {
            case accumulator: AverageAccumulator =>
                count += accumulator.count
                sum += accumulator.sum
            case _ => throw new IllegalArgumentException("The merged accumulator must be AverageAccumulator.")
        }
    }

    override def toString: String = s"AverageAccumulator ${getLocalValue} for ${count} elements"
}
