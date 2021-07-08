package com.hadoop.study.fraud.detect.sources

import org.apache.flink.util.Preconditions

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 14:24
 */

final class Throttler(var throttleBatchSize: Long = 0L, var nanosPerBatch: Long = 0L, var endOfNextBatchNanos: Long = 0L, var currentBatch: Long = 0L) {

    def this(maxRecordsPerSecond: Long, numberOfParallelSubtasks: Int) {
        this()
        Preconditions.checkArgument(maxRecordsPerSecond == -1 || maxRecordsPerSecond > 0, "maxRecordsPerSecond must be positive or -1 (infinite)")
        Preconditions.checkArgument(numberOfParallelSubtasks > 0, "numberOfParallelSubtasks must be greater than 0")
        // unlimited speed
        if (maxRecordsPerSecond == -1) {
            throttleBatchSize = -1
            nanosPerBatch = 0
            endOfNextBatchNanos = System.nanoTime + nanosPerBatch
            currentBatch = 0
            return
        }

        val ratePerSubtask = maxRecordsPerSecond.toFloat / numberOfParallelSubtasks
        // high rates: all throttling in intervals of 2ms
        if (ratePerSubtask >= 10000) {
            throttleBatchSize = ratePerSubtask.toInt / 500
            nanosPerBatch = 2000000L
        } else {
            throttleBatchSize = (ratePerSubtask / 20).toInt + 1
            nanosPerBatch = (1000000000L / ratePerSubtask).toInt * throttleBatchSize
        }

        this.endOfNextBatchNanos = System.nanoTime + nanosPerBatch
        this.currentBatch = 0
    }

    def throttle(): Unit = {
        if (throttleBatchSize == -1L) return

        if ( {
            currentBatch += 1;
            currentBatch
        } != throttleBatchSize) return

        currentBatch = 0
        val now = System.nanoTime

        val millisRemaining = ((endOfNextBatchNanos - now) / 1000000).toInt

        if (millisRemaining > 0) {
            endOfNextBatchNanos += nanosPerBatch
            Thread.sleep(millisRemaining)
        } else endOfNextBatchNanos = now + nanosPerBatch
    }
}
