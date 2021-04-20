package com.hadoop.study.scala.chapter25

import com.hadoop.study.scala.chapter25.RNA1.{M, N, S}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/20 19:10
 */

final class RNA1 private(val groups: Array[Int], val length: Int) extends IndexedSeq[Base] {

    def apply(idx: Int): Base = {
        if (idx < 0 || length <= idx)
            throw new IndexOutOfBoundsException
        Base.fromInt(groups(idx / N) >> (idx % N * S) & M)
    }
}

object RNA1 {

    // Number of bits necessary to represent group
    private val S = 2

    // Number of groups that fit in an Int
    private val N = 32 / S

    // Bitmask to isolate a group
    private val M = (1 << S) - 1

    def fromSeq(buf: Seq[Base]): RNA1 = {
        val groups = new Array[Int]((buf.length + N - 1) / N)
        for (i <- buf.indices)
            groups(i / N) |= Base.toInt(buf(i)) << (i % N * S)
        new RNA1(groups, buf.length)
    }

    def apply(bases: Base*) = fromSeq(bases)
}
