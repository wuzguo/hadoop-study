package com.sunvalley.study.scala

import com.sunvalley.study.scala.chapter4.ChecksumAccumulator.{ChecksumAccumulator, calculate}
import org.junit.{FixMethodOrder, Test}


@FixMethodOrder
class TestChapter4 {

    @Test
    def testChecksumAccumulator(): Unit = {
        val accumulator = new ChecksumAccumulator
        accumulator.add(2)
        println(accumulator.get() & 0xFF)
        println(accumulator.checksum())

        println(calculate("Every value is an object."))
    }
}
