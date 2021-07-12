package com.hadoop.study.fraud.detect.enums

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/12 8:54
 */


object AggregatorType extends Enumeration {
    type AggregatorType = Value

    val SUM, AVG, MIN, MAX = Value
}

object OperatorType extends Enumeration {
    type OperatorType = Value

    val EQUAL, NOT_EQUAL, GREATER_EQUAL, LESS_EQUAL, GREATER, LESS = Value
}

object RuleState extends Enumeration {
    type RuleState = Value

    val ACTIVE, PAUSE, DELETE, CONTROL = Value
}

object ControlType extends Enumeration {
    type ControlType = Value

    val CLEAR_STATE_ALL, DELETE_RULES_ALL, EXPORT_RULES_CURRENT = Value
}

object PaymentType extends Enumeration {
    type PaymentType = Value

    val CSH: Value = Value("CSH")
    val CRD: Value = Value("CRD")
}

object TransactionsType extends Enumeration {
    type TransactionsType = Value

    val GENERATOR, KAFKA = Value
}

object SourceType extends Enumeration {
    type SourceType = Value

    val KAFKA, PUBSUB, SOCKET, STATIC = Value
}

object SinkType extends Enumeration {
    type SinkType = Value

    val KAFKA, PUBSUB, STDOUT, DISCARD = Value
}