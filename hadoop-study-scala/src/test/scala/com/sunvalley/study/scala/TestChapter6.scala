package com.sunvalley.study.scala

import org.junit.{FixMethodOrder, Test}


@FixMethodOrder
class TestChapter6 {


    @Test
    def testRational(): Unit = {
        val oneHalf = new Rational(1, 2)
        println(oneHalf)

        val twoThirds = new Rational(2, 3)
        println(twoThirds)

        println(oneHalf.add(twoThirds))

        println(oneHalf * twoThirds)

        println(oneHalf + twoThirds)
    }
}
