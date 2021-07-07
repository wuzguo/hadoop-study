package com.hadoop.study.scala.streaming.state

import com.hadoop.study.scala.streaming.beans.{Action, Pattern}
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig

import java.util.Properties

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/7 15:29
 */

object State_BroadcastState_Case {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置并行度
        env.setParallelism(1)

        val properties = new Properties
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.20.0.92:9092")
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "broadcast-state")
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

        // 读取数据
        val actionStream = env.addSource(new FlinkKafkaConsumer("action-topic", new ActionEventSchema(), properties))
        val patternStream = env.addSource(new FlinkKafkaConsumer("pattern-topic", new PatternEventSchema(), properties))

        val patternDescriptor = new MapStateDescriptor[Void, Pattern]("pattern-state", classOf[Void], classOf[Pattern])
        val broadcastPatterns = patternStream.broadcast(patternDescriptor)

        val dataStream = actionStream.keyBy(_.userId).connect(broadcastPatterns).process(new PatternEvaluatorFunction(patternDescriptor))
        dataStream.print.setParallelism(1)

        env.execute("Streaming State Broadcast State")
    }


    class ActionEventSchema extends DeserializationSchema[Action] with SerializationSchema[Action] {

        override def deserialize(message: Array[Byte]): Action = {
            val value = new String(message).split(":")
            Action(value(0).toLong, value(1))
        }

        override def isEndOfStream(nextElement: Action): Boolean = false

        override def serialize(element: Action): Array[Byte] = element.toString.getBytes

        override def getProducedType: TypeInformation[Action] = TypeInformation.of(classOf[Action])
    }

    class PatternEventSchema extends DeserializationSchema[Pattern] with SerializationSchema[Pattern] {
        override def deserialize(message: Array[Byte]): Pattern = {
            val value = new String(message).split(":")
            Pattern(value(0), value(1))
        }

        override def isEndOfStream(nextElement: Pattern): Boolean = false

        override def serialize(element: Pattern): Array[Byte] = element.toString.getBytes

        override def getProducedType: TypeInformation[Pattern] = TypeInformation.of(classOf[Pattern])
    }

    class PatternEvaluatorFunction(patternDescriptor: MapStateDescriptor[Void, Pattern]) extends KeyedBroadcastProcessFunction[Long, Action, Pattern, (Long, Pattern)] {
        // handle for keyed state (per user)
        private var preActionState: ValueState[String] = _

        override def processElement(value: Action, ctx: KeyedBroadcastProcessFunction[Long, Action, Pattern, (Long,
          Pattern)]#ReadOnlyContext, out: Collector[(Long, Pattern)]): Unit = {
            // access MapState with null as VOID default value
            val pattern = ctx.getBroadcastState(this.patternDescriptor).get(null)

            // get previous action of current user from keyed state
            val preAction = preActionState.value
            // user had an action before, check if pattern matches
            if (pattern != null && preAction != null) {
                if (pattern.preAction.equals(preAction) && pattern.action.equals(value.action)) {
                    out.collect((ctx.getCurrentKey, pattern))
                }
            }
            // update keyed state and remember action for next pattern evaluation
            preActionState.update(value.action)
        }

        override def processBroadcastElement(value: Pattern, ctx: KeyedBroadcastProcessFunction[Long, Action,
          Pattern, (Long, Pattern)]#Context, out: Collector[(Long, Pattern)]): Unit = {
            // store the new pattern by updating the broadcast state
            val bcState = ctx.getBroadcastState(patternDescriptor)
            // storing in MapState with null as VOID default value
            bcState.put(null, value)
        }

        override def open(parameters: Configuration): Unit = {
            // initialize keyed state
            preActionState = getRuntimeContext().getState(new ValueStateDescriptor[String]("last-action-state", classOf[String]))
        }
    }
}
