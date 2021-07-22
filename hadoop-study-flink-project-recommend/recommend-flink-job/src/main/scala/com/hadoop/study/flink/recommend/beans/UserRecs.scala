package com.hadoop.study.flink.recommend.beans

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/21 9:39
 */

// 定义用户的推荐列表
case class UserRecs(userId: Int, recs: Seq[Recommendation])
