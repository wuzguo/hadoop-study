package com.sunvalley.study.scala.chapter10

class Tiger2(param1: Boolean, param2: Int) extends Cat {

    override val dangerous: Boolean = param1

    private val age: Int = param2
}
