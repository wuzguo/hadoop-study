package com.sunvalley.study.scala

import com.sunvalley.study.scala.chapter6.Rational
import org.junit.{FixMethodOrder, Test}


@FixMethodOrder
class TestChapter6 {

    implicit def intToRational(x: Int): Rational = new Rational(x)

    @Test
    def testRational(): Unit = {
        val oneHalf = new Rational(1, 2)
        println(oneHalf)

        val twoThirds = new Rational(2, 3)
        println(twoThirds)

        println(oneHalf.add(twoThirds))

        println(oneHalf * twoThirds)

        println(oneHalf + twoThirds)

        val r = new Rational(3, 4)

        // 3/2
        println(r * 2)

        // overloaded method * with alternatives:
        //  (x: Double)Double <and>
        //  (x: Float)Float <and>
        //  (x: Long)Long <and>
        //  (x: Int)Int <and>
        //  (x: Char)Int <and>
        //  (x: Short)Int <and>
        //  (x: Byte)Int
        // cannot be applied to (com.sunvalley.study.scala.chapter6.Rational)
        //        println(2 * r)

        //  implicit def intToRational(x: Int) = new Rational(x)
        // 增加这行隐式转换，自动将整数转换为Rational
        println(2 * r)
    }
}
