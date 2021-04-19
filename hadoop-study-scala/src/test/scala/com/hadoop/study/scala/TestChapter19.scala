package com.sunvalley.study.scala

import com.sunvalley.study.scala.chapter19.Person
import org.junit.{FixMethodOrder, Test}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/8 16:40
 */

@FixMethodOrder
class TestChapter19 {

    def orderedMergeSort[T <: Ordered[T]](xs: List[T]): List[T] = {
        def merge(xs: List[T], ys: List[T]): List[T] =
            (xs, ys) match {
                case (Nil, _) => ys
                case (_, Nil) => xs
                case (x :: xs1, y :: ys1) =>
                    if (x < y) x :: merge(xs1, ys)
                    else y :: merge(xs, ys1)
            }

        val n = xs.length / 2
        if (n == 0) xs
        else {
            val (ys, zs) = xs splitAt n
            merge(orderedMergeSort(ys), orderedMergeSort(zs))
        }
    }

    @Test
    def testOrderPerson(): Unit = {
        val people = List(
            new Person("Larry", "Wall"),
            new Person("Anders", "Hejlsberg"),
            new Person("Guido", "van Rossum"),
            new Person("Alan", "Kay"),
            new Person("Yukihiro", "Matsumoto")
        )
        val sortedPeople = orderedMergeSort(people)
        println(sortedPeople)

        // inferred type arguments [Int] do not conform to method orderedMergeSort's type parameter bounds [T <: Ordered[T]]
        //        val wontCompile = orderedMergeSort(List(3, 2, 1))
        // val wontCompile = orderedMergeSort(List(3, 2, 1))
        // println(wontCompile)
    }
}
