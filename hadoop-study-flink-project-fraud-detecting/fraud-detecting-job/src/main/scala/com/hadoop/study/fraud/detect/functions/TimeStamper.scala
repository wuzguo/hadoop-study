package com.hadoop.study.fraud.detect.functions

import com.hadoop.study.fraud.detect.dynamic.TimestampAssignable
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 16:46
 */

class TimeStamper[T <: TimestampAssignable[Long]] extends RichFlatMapFunction[T, T] {

    override def flatMap(value: T, out: Collector[T]): Unit = {
        value.assignIngestionTimestamp(System.currentTimeMillis)
        out.collect(value)
    }
}
