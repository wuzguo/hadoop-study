package com.sunvalley.study.scala.chapter12

trait Incrementing extends IntQueue {

    abstract override def put(x: Int) = { super.put(x + 1) }
}