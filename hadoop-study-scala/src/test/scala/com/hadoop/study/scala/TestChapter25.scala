package com.hadoop.study.scala

import com.hadoop.study.scala.chapter25._
import org.junit.{FixMethodOrder, Test}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/20 19:12
 */

@FixMethodOrder
class TestChapter25 {

    @Test
    def testBase(): Unit = {
        val xs = List(A, G, T, A)
        println(RNA1.fromSeq(xs))
        val rna1 = RNA1(A, U, G, G, T)
        println(rna1.length)
        println(rna1.last)
        println(rna1.take(3))
    }
}
