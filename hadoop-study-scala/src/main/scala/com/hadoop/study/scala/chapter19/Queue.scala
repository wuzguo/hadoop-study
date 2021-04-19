package com.sunvalley.study.scala.chapter19

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/8 16:32
 */

class Queue[T](private val leading: List[T], private val trailing: List[T]) {
    private def mirror =
        if (leading.isEmpty)
            new Queue(trailing.reverse, Nil)
        else
            this

    def head = mirror.leading.head

    def tail = {
        val q = mirror
        new Queue(q.leading.tail, q.trailing)
    }

    def enqueue(x: T) =
        new Queue(leading, x :: trailing)
}
