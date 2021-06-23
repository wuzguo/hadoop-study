package com.hadoop.study.scala.example.beans

import java.sql.Timestamp

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/23 10:25
 */

case class AdClickProvinceCount(windowEnd: Timestamp, province: String, count: Long) {

    override def toString: String = s"${windowEnd}：[${province}] ${count}"
}
