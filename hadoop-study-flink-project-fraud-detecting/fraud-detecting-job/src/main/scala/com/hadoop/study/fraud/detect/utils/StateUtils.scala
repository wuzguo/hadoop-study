package com.hadoop.study.fraud.detect.utils

import com.hadoop.study.fraud.detect.beans.Rule
import com.hadoop.study.fraud.detect.enums.RuleState
import com.hadoop.study.fraud.detect.enums.RuleState.{ACTIVE, DELETE, PAUSE}
import org.apache.flink.api.common.state.{BroadcastState, MapState}
import org.slf4j.LoggerFactory

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 16:26
 */

object StateUtils {

    private val log = LoggerFactory.getLogger("StateUtils")

    def handleBroadcast(rule: Rule, broadcastState: BroadcastState[Int, Rule]): Unit = {
        RuleState.withName(rule.ruleState) match {
            case ACTIVE =>
                broadcastState.put(rule.ruleId, rule)
            case PAUSE =>
                broadcastState.put(rule.ruleId, rule)
            case DELETE =>
                broadcastState.remove(rule.ruleId)
            case _ =>
                log.info(s"unsupported rule state type: ${rule.ruleState}")
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
