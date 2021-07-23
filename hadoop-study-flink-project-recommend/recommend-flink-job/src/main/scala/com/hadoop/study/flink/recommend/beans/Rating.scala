package com.hadoop.study.flink.recommend.beans

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/20 14:34
 */


/**
 * Rating数据集
 * 4867        用户ID
 * 457976      商品ID
 * 5.0         评分
 * 1395676800  时间戳
 */
case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int, createTime: Long) {

    override def toString: String = s"(userId: ${userId}, productId: ${productId}, score: ${score}, timestamp: ${timestamp}, createTime: ${createTime})"
}
