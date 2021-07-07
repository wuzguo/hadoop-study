package com.hadoop.study.scala.streaming.state

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction

import scala.collection.mutable.ListBuffer

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/7 14:14
 */

object State_OperatorState_Case {

    def main(args: Array[String]): Unit = {

    }

    class BufferingSinkFunction(threshold: Int) extends SinkFunction[(String, Int)] with CheckpointedFunction {

        private var checkpointState: ListState[(String, Int)] = _

        private val bufferedElements = ListBuffer[(String, Int)]()


        override def invoke(value: (String, Int), context: SinkFunction.Context): Unit = {
            bufferedElements.append(value)
            if (bufferedElements.size == threshold) {
                for (element <- bufferedElements) {
                    // send it to the sink
                }
                bufferedElements.clear
            }
        }

        override def snapshotState(context: FunctionSnapshotContext): Unit = {
            checkpointState.clear()
            bufferedElements.foreach(_ => checkpointState.add(_))
        }

        override def initializeState(context: FunctionInitializationContext): Unit = {
            val descriptor = new ListStateDescriptor[(String, Int)]("buffered-elements", TypeInformation.of(new TypeHint[(String, Int)] {}))
            checkpointState = context.getOperatorStateStore.getListState(descriptor)

            if (context.isRestored) {
                checkpointState.get.forEach(_ => bufferedElements.append(_))
            }
        }
    }
}
