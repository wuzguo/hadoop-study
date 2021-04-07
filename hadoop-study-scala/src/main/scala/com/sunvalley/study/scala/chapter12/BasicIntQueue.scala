package com.sunvalley.study.scala.chapter12

import scala.collection.mutable.ArrayBuffer

class BasicIntQueue extends IntQueue {

    private val buf = new ArrayBuffer[Int]

    def get() = buf.remove(0)

    def put(x: Int) = buf += x
}