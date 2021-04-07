package com.sunvalley.study.scala

import com.sunvalley.study.scala.chapter2.MyClass
import org.junit.{FixMethodOrder, Test}

import java.math.BigInteger
import scala.collection.immutable.HashMap


@FixMethodOrder
class TestChapter1 {

    @Test
    def testCapital(): Unit = {
        var capital = Map("US" -> "Washington", "France" -> "Paris")
        capital += ("Japan" -> "Tokyo")
        println(capital("Japan"))
        println(capital.getClass)
    }

    def factorial(x: BigInt): BigInt =
        if (x == 0) 1 else x * factorial(x - 1)

    @Test
    def testFactorial(): Unit = {
        println(factorial(100))
    }

    def factorial2(x: BigInteger): BigInteger =
        if (x == BigInteger.ZERO)
            BigInteger.ONE
        else
            x.multiply(factorial2(x.subtract(BigInteger.ONE)))

    @Test
    def testFactorial2(): Unit = {
        println(factorial2(BigInteger.valueOf(100)))
    }

    @Test
    def testMyClass(): Unit = {
        println(new MyClass(1, "Tomcat"))
    }

    def hasUpper(name: String): Boolean = {
        name.exists(_.isUpper)
    }

    @Test
    def testHasUpper(): Unit = {
        println(hasUpper("Tomcat"))
    }

    @Test
    def testMap(): Unit = {
        var x: HashMap[Int, String] = new HashMap[Int, String]();
        println(x)
        val xl = new HashMap[Int, String]()
        println(xl)
    }
}
