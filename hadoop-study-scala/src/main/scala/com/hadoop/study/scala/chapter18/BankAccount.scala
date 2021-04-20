package com.hadoop.study.scala.chapter18

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/8 16:00
 */

class BankAccount {

    private var bal: Int = 0

    def balance: Int = bal

    def deposit(amount: Int) = {
        require(amount > 0)
        bal += amount
    }

    def withdraw(amount: Int): Boolean =
        if (amount > bal) false
        else {
            bal -= amount
            true
        }
}
