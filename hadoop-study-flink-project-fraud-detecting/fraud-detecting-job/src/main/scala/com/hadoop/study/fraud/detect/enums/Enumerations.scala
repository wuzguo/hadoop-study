package com.hadoop.study.fraud.detect.enums

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/12 8:54
 */


object AggregateType extends Enumeration {
    type Aggregate = Value

    val SUM, AVG, MIN, MAX = Value
}

object OperateType extends Enumeration {
    type Operate = Value

    val EQUAL, NOT_EQUAL, GREATER_EQUAL, LESS_EQUAL, GREATER, LESS = Value
}

object RuleState extends Enumeration {
    type State = Value

    val ACTIVE, PAUSE, DELETE, CONTROL = Value
}

object ControlType extends Enumeration {
    type Control = Value

    val CLEAR_STATE_ALL, DELETE_RULES_ALL, EXPORT_RULES_CURRENT = Value
}

object PaymentType extends Enumeration {
    type Payment = Value

    val CSH: Value = Value("CSH")
    val CRD: Value = Value("CRD")
}

object SourceType extends Enumeration {
    type Source = Value

    val KAFKA, PUBSUB, SOCKET, STATIC, GENERATOR = Value
}

object SinkType extends Enumeration {
    type Sink = Value

    val KAFKA, PUBSUB, STDOUT, DISCARD = Value
}