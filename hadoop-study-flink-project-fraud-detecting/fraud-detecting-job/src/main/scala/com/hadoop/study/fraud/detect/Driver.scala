package com.hadoop.study.fraud.detect

import com.hadoop.study.fraud.detect.config.Parameters.{BOOL_PARAMS, INT_PARAMS, STRING_PARAMS}
import com.hadoop.study.fraud.detect.config.{Config, Parameters}
import com.hadoop.study.fraud.detect.dynamic.RulesEvaluator
import org.apache.flink.api.java.utils.ParameterTool

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/9 9:32
 */

object Driver extends App {

    val tool = ParameterTool.fromArgs(args)
    val inputParams = new Parameters(tool)
    val config = new Config(inputParams, STRING_PARAMS, INT_PARAMS, BOOL_PARAMS)
    val rulesEvaluator = RulesEvaluator(config)
    rulesEvaluator.run()
}
