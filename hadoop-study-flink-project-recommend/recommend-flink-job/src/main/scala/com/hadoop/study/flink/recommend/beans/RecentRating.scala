package com.hadoop.study.flink.recommend.beans

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/20 14:34
 */

case class RecentRating(userId: Int, productId: Int, yearMonth: Int) {

    override def toString: String = s"(userId: ${userId}, productId: ${productId}, yearMonth: ${yearMonth})"
}
