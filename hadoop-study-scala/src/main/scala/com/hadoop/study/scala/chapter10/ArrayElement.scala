package com.sunvalley.study.scala.chapter10

class ArrayElement(conts: Array[String]) extends Element {

    override def contents: Array[String] = conts

    override def demo(): Unit = println("ArrayElement's implementation invoked")
}
