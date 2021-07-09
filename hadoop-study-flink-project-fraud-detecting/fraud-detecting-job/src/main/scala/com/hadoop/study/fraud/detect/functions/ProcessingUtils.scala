package com.hadoop.study.fraud.detect.functions

import com.hadoop.study.fraud.detect.beans.Rule
import com.hadoop.study.fraud.detect.beans.RuleState._
import org.apache.flink.api.common.state.{BroadcastState, MapState}

import scala.collection.mutable

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 16:26
 */

object ProcessingUtils {

    def handleRuleBroadcast(rule: Rule, broadcastState: BroadcastState[Int, Rule]): Unit = {
        rule.ruleState match {
            case ACTIVE =>
            case PAUSE =>
                broadcastState.put(rule.ruleId, rule)
            case DELETE =>
                broadcastState.remove(rule.ruleId)
        }
    }

    def addToStateValuesSet[K, V](mapState: MapState[K, mutable.Set[V]], key: K, value: V): mutable.Set[V] = {
        var values = mapState.get(key)
        if (values != null) values.add(value)
        else {
            values = mutable.Set[V]()
            values.add(value)
        }
        mapState.put(key, values)
        values
    }
}
