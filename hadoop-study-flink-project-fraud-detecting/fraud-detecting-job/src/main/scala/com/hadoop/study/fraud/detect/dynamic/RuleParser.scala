package com.hadoop.study.fraud.detect.dynamic

import com.fasterxml.jackson.databind.ObjectMapper
import com.hadoop.study.fraud.detect.beans.{AggregatorType, OperatorType, Rule, RuleState}
import com.hadoop.study.fraud.detect.dynamic.RuleParser.parsePlain

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
        if (tokens.size ne 8) throw new IOException(s"Invalid rule (wrong number of tokens): ${rules}")

        val iter = tokens.iterator
        Rule(
            stripBrackets(iter.next).toInt,
            RuleState.withName(stripBrackets(iter.next).toUpperCase),
            getNames(iter.next),
            stripBrackets(iter.next),
            AggregatorType.withName(stripBrackets(iter.next).toUpperCase),
            OperatorType.withName(stripBrackets(iter.next)),
            BigDecimal(stripBrackets(iter.next)),
            stripBrackets(iter.next).toInt
        )
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