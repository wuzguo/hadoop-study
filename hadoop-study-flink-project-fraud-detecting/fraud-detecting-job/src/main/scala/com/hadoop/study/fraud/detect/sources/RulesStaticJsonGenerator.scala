package com.hadoop.study.fraud.detect.sources

import com.hadoop.study.fraud.detect.sources.RulesStaticJsonGenerator.RULES

import java.util.SplittableRandom

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 15:21
 */

case class RulesStaticJsonGenerator() extends BaseGenerator[String] {

    def randomEvent(rnd: SplittableRandom, id: Long): String = if (id >= 0 && id < RULES.length) RULES(id.toInt) else null
}

object RulesStaticJsonGenerator {

    private val RULES = Array[String](
        "{ruleId:1,aggregateFieldName:paymentAmount,aggregatorFunctionType:SUM,groupingKeyNames:[payeeId, beneficiaryId],limit:20000000,limitOperatorType:GREATER,ruleState:ACTIVE,windowMinutes:43200}",
        "{ruleId:2,aggregateFieldName:COUNT_FLINK,aggregatorFunctionType:SUM,groupingKeyNames:[paymentType],limit:300,limitOperatorType:LESS,ruleState:PAUSE,windowMinutes:1440}",
        "{ruleId:3,aggregateFieldName:paymentAmount,aggregatorFunctionType:SUM,groupingKeyNames:[beneficiaryId],limit:10000000,limitOperatorType:GREATER_EQUAL,ruleState:ACTIVE,windowMinutes:1440}",
        "{ruleId:4,aggregateFieldName:COUNT_WITH_RESET_FLINK,aggregatorFunctionType:SUM,groupingKeyNames:[paymentType],limit:100,limitOperatorType:GREATER_EQUAL,ruleState:ACTIVE,windowMinutes:1440}")

}
