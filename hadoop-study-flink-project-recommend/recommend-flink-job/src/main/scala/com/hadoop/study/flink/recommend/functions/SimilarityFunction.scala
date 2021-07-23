package com.hadoop.study.flink.recommend.functions

import org.apache.flink.table.functions.ScalarFunction

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/23 14:53
 */

case class SimilarityFunction() extends ScalarFunction {

    def eval(product1: Int, product2: Int, coCount: Long, count1: Integer, count2: Integer): (Int, Int, Double) = {
        val coSim = coCount / Math.sqrt(count1 * count2)
        (product1, product2, coSim)
    }
}
