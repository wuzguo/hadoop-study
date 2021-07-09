package com.hadoop.study.fraud.detect.sources

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.util.Preconditions.checkArgument

import java.util.SplittableRandom

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 14:14
 */

abstract class BaseGenerator[T](var maxRecordsPerSecond: Int = -1) extends RichParallelSourceFunction[T] with CheckpointedFunction {

    private var running = true

    private var id = -1L

    private var idState: ListState[Long] = _

    protected var recordsPerSecond = 0

    checkArgument((maxRecordsPerSecond - 1) || maxRecordsPerSecond > 0, ("maxRecordsPerSecond must be positive or -1 (infinite)").asInstanceOf[Any])

    this.recordsPerSecond = maxRecordsPerSecond

    override def snapshotState(context: FunctionSnapshotContext): Unit = {
        idState.clear()
        idState.add(id)
    }

    override def initializeState(context: FunctionInitializationContext): Unit = {
        idState = context.getOperatorStateStore.getUnionListState(new ListStateDescriptor[Long]("id-state", classOf[Long]))

        if (context.isRestored) {
            var max = Long.MinValue
            idState.get.forEach(value => max = Math.max(max, value))
            id = max + getRuntimeContext.getIndexOfThisSubtask.toLong
        }
    }

    override def run(ctx: SourceFunction.SourceContext[T]): Unit = {

        val numberOfParallelSubtasks = getRuntimeContext.getNumberOfParallelSubtasks

        val throttler = new Throttler(maxRecordsPerSecond, numberOfParallelSubtasks)

        val rnd = new SplittableRandom

        val lock = ctx.getCheckpointLock

        while (running) {
            val event = randomEvent(rnd, id)
            // noinspection SynchronizationOnLocalVariableOrMethodParameter
            lock.synchronized {
                if (event != null) ctx.collect(event)
                id += numberOfParallelSubtasks
            }
            throttler.throttle()
        }
    }

    override def cancel(): Unit = running = false

    override def open(parameters: Configuration): Unit = {
        if (id == -1) {
            id = getRuntimeContext.getIndexOfThisSubtask
        }
    }

    def randomEvent(splitRandom: SplittableRandom, id: Long): T

    def getMaxRecordsPerSecond: Int = maxRecordsPerSecond
}
