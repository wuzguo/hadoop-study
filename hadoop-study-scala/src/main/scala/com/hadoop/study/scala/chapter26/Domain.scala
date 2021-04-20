package com.hadoop.study.scala.chapter26

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/20 19:47
 */

object Domain {

    // The injection method (optional)
    def apply(parts: String*): String =
        parts.reverse.mkString(".")

    // The extraction method (mandatory)
    def unapplySeq(whole: String): Option[Seq[String]] =
        Some(whole.split("\\.").reverse)
}