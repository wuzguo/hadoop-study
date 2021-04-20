package com.hadoop.study.scala.chapter11

class SwissFrancs(val amount: Int) extends AnyVal {

    override def toString() = amount + " CHF"
}