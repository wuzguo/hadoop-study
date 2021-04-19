package com.sunvalley.study.scala

import com.sunvalley.study.scala.chapter18.BankAccount
import com.sunvalley.study.scala.chapter18.MySimulation.{Wire, halfAdder, probe, run}
import org.junit.{FixMethodOrder, Test}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/8 15:59
 */

@FixMethodOrder
class TestChapter18 {

    @Test
    def testMutableObject(): Unit = {
        val account = new BankAccount
        account deposit 100
        // true
        println(account withdraw 80)
        // false
        println(account withdraw 80)
    }


    @Test
    def testMySimulation(): Unit = {
        val input1, input2, sum, carry = new Wire
        probe("sum", sum)
        probe("carry", carry)
        halfAdder(input1, input2, sum, carry)

        input1 setSignal true
        run()
        input2 setSignal true
        run()
    }
}
