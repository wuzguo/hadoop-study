package com.sunvalley.study.scala.chapter10

class Tiger(
             override val dangerous: Boolean,
             private val age: Int
           ) extends Cat
