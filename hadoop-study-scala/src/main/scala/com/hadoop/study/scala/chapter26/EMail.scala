package com.hadoop.study.scala.chapter26

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/20 19:41
 */

object EMail extends ((String, String) => String) {

    // The injection method (optional)
    def apply(user: String, domain: String) = user + "@" + domain

    // The extraction method (mandatory)
    def unapply(str: String): Option[(String, String)] = {
        val parts = str split "@"
        if (parts.length == 2) Some(parts(0), parts(1)) else None
    }
}
