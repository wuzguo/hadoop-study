package com.sunvalley.study.scala

import com.sunvalley.study.scala.chapter18.BankAccount
import org.junit.Test

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/8 15:59
 */

class TestChapter18 {

    @Test
    def testMutableObject(): Unit = {
        val account = new BankAccount
        account deposit 100
        // true
        println( account withdraw 80)
        // false
        println(account withdraw 80)
    }
}
