package com.hadoop.study.fraud.detect.functions

import com.hadoop.study.fraud.detect.beans.Rule
import com.hadoop.study.fraud.detect.beans.RuleState._
import org.apache.flink.api.common.state.{BroadcastState, MapState}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 16:26
 */

object ProcessingUtils {

    def handleBroadcast(rule: Rule, broadcastState: BroadcastState[Int, Rule]): Unit = {
        rule.ruleState match {
            case ACTIVE =>
            case PAUSE =>
                broadcastState.put(rule.ruleId, rule)
            case DELETE =>
                broadcastState.remove(rule.ruleId)
        }
    }

    def addValues[K, V](mapState: MapState[K, Set[V]], key: K, value: V): Set[V] = {
        var values = mapState.get(key)
        if (values != null) values += value
        else {
            values = Set[V]()
            values += value
        }
        mapState.put(key, values)
        values
    }
}
