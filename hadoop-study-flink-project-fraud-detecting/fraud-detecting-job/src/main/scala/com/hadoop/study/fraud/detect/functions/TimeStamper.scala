package com.hadoop.study.fraud.detect.functions

import com.hadoop.study.fraud.detect.dynamic.TimestampAssignable
import org.apache.flink.api.common.functions.MapFunction

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 16:46
 */

class TimeStamper[T <: TimestampAssignable[Long]] extends MapFunction[T, T] {

    override def map(value: T): T = {
        value.assignIngestionTimestamp(System.currentTimeMillis)
        value
    }
}
