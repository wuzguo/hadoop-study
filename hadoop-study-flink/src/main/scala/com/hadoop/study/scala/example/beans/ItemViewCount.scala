package com.hadoop.study.scala.example.beans

import java.sql.Timestamp

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/21 19:25
 */

case class ItemViewCount(itemId: Long, windowEnd: Long, timestamp: Timestamp, count: Long)
