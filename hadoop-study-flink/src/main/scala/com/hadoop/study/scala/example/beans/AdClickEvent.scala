package com.hadoop.study.scala.example.beans

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/23 10:25
 */

case class AdClickEvent(userId: Long, adId: Long, province: String, city: String, timestamp: Long)
