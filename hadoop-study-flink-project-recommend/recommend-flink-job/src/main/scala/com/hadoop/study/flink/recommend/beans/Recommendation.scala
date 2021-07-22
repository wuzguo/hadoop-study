package com.hadoop.study.flink.recommend.beans

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/21 9:39
 */

// 定义标准推荐对象
case class Recommendation(productId: Int, score: Double)
