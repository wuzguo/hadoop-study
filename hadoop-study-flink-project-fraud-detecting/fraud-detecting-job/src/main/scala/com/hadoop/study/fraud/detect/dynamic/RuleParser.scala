package com.hadoop.study.fraud.detect.dynamic

import com.fasterxml.jackson.databind.ObjectMapper
import com.hadoop.study.fraud.detect.beans.Rule
import com.hadoop.study.fraud.detect.dynamic.RuleParser.parsePlain
import com.hadoop.study.fraud.detect.enums.{AggregatorType, OperatorType, RuleState}

import java.io.IOException

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 14:36
 */

case class RuleParser() {

    private val mapper = new ObjectMapper()

    def from(line: String): Rule = if (line.nonEmpty && '{' == line.charAt(0)) parseJson(line) else parsePlain(line)

    private def parseJson(ruleString: String): Rule = mapper.readValue(ruleString, classOf[Nothing])
}

object RuleParser {

    private def parsePlain(rules: String): Rule = {
        val tokens = rules.split(",")
        if (tokens.size != 8) throw new IOException(s"Invalid rule (wrong number of tokens): ${rules}")

        val iter = tokens.iterator
        val rule = new Rule()
        rule.ruleId = stripBrackets(iter.next).toInt
        rule.ruleState = RuleState.withName(stripBrackets(iter.next).toUpperCase)
        rule.groupingKeyNames = getNames(iter.next)
        rule.aggregateFieldName = stripBrackets(iter.next)
        rule.aggregatorType = AggregatorType.withName(stripBrackets(iter.next).toUpperCase)
        rule.limitOperatorType = OperatorType.withName(stripBrackets(iter.next))
        rule.limit = BigDecimal(stripBrackets(iter.next))
        rule.windowMinutes = stripBrackets(iter.next).toInt
        rule
    }

    private def stripBrackets(expr: String) = expr.replaceAll("[()]", "")

    private def getNames(expr: String): List[String] = {
        val keyNames = stripBrackets(expr)

        if (!("" == keyNames)) {
            val tokens = keyNames.split("&", -1)
            tokens.toList
        } else List[String]()
    }
}