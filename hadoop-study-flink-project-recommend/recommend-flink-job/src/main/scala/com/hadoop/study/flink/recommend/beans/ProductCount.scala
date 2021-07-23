package com.hadoop.study.flink.recommend.beans

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/20 14:33
 */

case class ProductCount(userId: Int, productId: Int, score: Double, count: Int)
