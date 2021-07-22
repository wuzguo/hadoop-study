package com.hadoop.study.flink.recommend.beans

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/21 9:37
 */

case class ProductRating(userId: Int, productId: Int, score: Double, timestamp: Int)
