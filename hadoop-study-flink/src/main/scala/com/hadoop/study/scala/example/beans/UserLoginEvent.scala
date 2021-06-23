package com.hadoop.study.scala.example.beans

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/23 14:32
 */

case class UserLoginEvent(userId: Long, host: String, result: String, timestamp: Long)
