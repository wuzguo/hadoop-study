package com.sunvalley.study.scala

import org.junit.{FixMethodOrder, Test}

@FixMethodOrder
class TestChapter5 {

    @Test
    def testInterpolation(): Unit = {
        val name = "reader"
        // Hello, reader
        println(s"Hello, $name")

        // Hello, The answer is 42
        println(s"Hello, The answer is ${ 6 * 7}")

        // Hello, The answer is ${ 6 * 7}
        println("Hello, The answer is ${ 6 * 7}")

        // No\\escape!
        println(s"No\\\\escape!")
        // No\\\\escape!
        println(raw"No\\\\escape!")
        // Hello, reader
        println(raw"Hello, $name")

        // 3.14159
        println(f"${math.Pi}%.5f")
        // 3.141592653589793%.5f
        println(s"${math.Pi}%.5f")
    }
}
