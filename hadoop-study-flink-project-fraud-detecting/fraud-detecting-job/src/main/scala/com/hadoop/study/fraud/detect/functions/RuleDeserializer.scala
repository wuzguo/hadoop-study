package com.hadoop.study.fraud.detect.functions

import com.hadoop.study.fraud.detect.beans.Rule
import com.hadoop.study.fraud.detect.dynamic.RuleParser
import com.hadoop.study.fraud.detect.enums.RuleState
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 14:35
 */

case class RuleDeserializer() extends RichFlatMapFunction[String, Rule] {

    private var ruleParser: RuleParser = _

    override def flatMap(value: String, out: Collector[Rule]): Unit = {
        val rule: Rule = ruleParser.from(value)

        if ((rule.ruleState ne RuleState.CONTROL) && rule.ruleId == 0) {
            throw new NullPointerException("ruleId cannot be null: " + rule.toString)
        }
        out.collect(rule)
    }

    override def open(parameters: Configuration): Unit = ruleParser = RuleParser()
}
