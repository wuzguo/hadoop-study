package com.hadoop.study.flink.analysis.beans

import java.sql.Timestamp

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/21 19:20
 */

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, action: String, timestamp: Timestamp)
