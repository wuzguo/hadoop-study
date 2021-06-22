package com.hadoop.study.scala.example.beans

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/22 15:10
 */

case class PageViewEvent(host: String, timestamp: Long, method: String, url: String)
