package com.hadoop.study.flink.recommend.functions

import com.hadoop.study.flink.recommend.beans.CoSimProduct
import org.apache.flink.table.functions.ScalarFunction

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/23 14:53
 */

class SimilarityFunction extends ScalarFunction {

    def eval(product1: Int, product2: Int, coCount: BigDecimal, count1: Int, count2: Int): CoSimProduct = {
        val coSim = coCount.toDouble / Math.sqrt(count1 * count2)
        CoSimProduct(product1, product2, coSim)
    }
}
