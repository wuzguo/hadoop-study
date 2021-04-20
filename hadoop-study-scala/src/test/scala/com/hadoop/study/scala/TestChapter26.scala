package com.hadoop.study.scala

import com.hadoop.study.scala.chapter26._
import org.junit.{FixMethodOrder, Test}

import scala.util.matching.Regex

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/20 19:42
 */

@FixMethodOrder
class TestChapter26 {

    @Test
    def testEmail(): Unit = {
        println(EMail.unapply("John@epfl.ch") equals Some("John", "epfl.ch"))
        println(EMail.unapply("John Doe") equals None)
    }

    def userTwiceUpper(s: String) = s match {
        case EMail(Twice(x@UpperCase()), domain) =>
            "match: " + x + " in domain " + domain
        case _ =>
            "no match"
    }

    @Test
    def testUpper(): Unit = {
        println(userTwiceUpper("DIDI@hotmail.com"))
        println(userTwiceUpper("DIDO@hotmail.com"))
        println(userTwiceUpper("didi@hotmail.com"))
    }

    def isTomInDotCom(s: String): Boolean = s match {
        case EMail("tom", Domain("com", _*)) => true
        case _ => false
    }

    @Test
    def testDotCom(): Unit = {
        println(isTomInDotCom("tom@sun.com"))
        println(isTomInDotCom("peter@sun.com"))
        println(isTomInDotCom("tom@acm.org"))
    }

    @Test
    def testRegular(): Unit = {
        val decimal = new Regex("(-)?(\\d+)(\\.\\d*)?")
        println(decimal)
        println(decimal.matches("01234"))
    }
}
