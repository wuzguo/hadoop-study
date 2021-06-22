package com.hadoop.study.scala.example.beans

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/22 15:28
 */

// 定义窗口聚合结果样例类
case class PageViewCount(url: String, windowEnd: Long, count: Long)