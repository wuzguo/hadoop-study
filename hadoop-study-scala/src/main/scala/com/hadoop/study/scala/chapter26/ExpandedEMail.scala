package com.hadoop.study.scala.chapter26

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/20 19:49
 */

object ExpandedEMail {
    def unapplySeq(email: String)
    : Option[(String, Seq[String])] = {
        val parts = email split "@"
        if (parts.length == 2)
            Some(parts(0), parts(1).split("\\.").reverse)
        else
            None
    }
}
