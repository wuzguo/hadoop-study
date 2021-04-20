package com.hadoop.study.scala

import com.hadoop.study.scala.chapter28.CCTherm
import org.junit.{FixMethodOrder, Test}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/20 20:09
 */

@FixMethodOrder
class TestChapter28 {

    @Test
    def testCCTherm(): Unit = {
        val therm = new CCTherm {
            val description = "hot dog #5"
            val yearMade = 1952
            val dateObtained = "March 14, 2006"
            val bookPrice = 2199
            val purchasePrice = 500
            val condition = 9
        }
        println(therm)
    }
}
