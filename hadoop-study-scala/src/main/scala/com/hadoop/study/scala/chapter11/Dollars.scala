package com.sunvalley.study.scala.chapter11


class Dollars(val amount: Int) extends AnyVal {

    override def toString() = "$" + amount
}

