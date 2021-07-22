package com.hadoop.study.flink.recommend.beans

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/21 9:39
 */

// 定义商品相似度列表
case class ProductRecs(productId: Int, recs: Seq[Recommendation])
