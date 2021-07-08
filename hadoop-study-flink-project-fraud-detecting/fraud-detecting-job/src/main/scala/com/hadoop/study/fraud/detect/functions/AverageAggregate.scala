package com.hadoop.study.fraud.detect.functions

import org.apache.flink.api.common.functions.AggregateFunction

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 13:45
 */

class AverageAggregate extends AggregateFunction[Long, (Long, Long), Double] {

    override def createAccumulator(): (Long, Long) = (0, 0)

    override def add(value: Long, accumulator: (Long, Long)): (Long, Long) = (accumulator._1 + value, accumulator._2 + 1)

    override def getResult(accumulator: (Long, Long)): Double = accumulator._1 / accumulator._2

    override def merge(pre: (Long, Long), next: (Long, Long)): (Long, Long) = (pre._1 + next._1, pre._2 + next._2)
}
