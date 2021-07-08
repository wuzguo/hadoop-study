package com.hadoop.study.fraud.detect.dynamic

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 13:51
 */

trait TimestampAssignable[T] {

    def assignIngestionTimestamp(timestamp: T): Unit
}
