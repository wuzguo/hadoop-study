package com.sunvalley.study.scala

import org.junit.{FixMethodOrder, Test}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/8 14:23
 */

@FixMethodOrder
class TestChapter16 {

    @Test
    def testList(): Unit = {
        val fruit = List("apples", "oranges", "pears")
        val nums = List(1, 2, 3, 4)
        val diag3 =
            List(
                List(1, 0, 0),
                List(0, 1, 0),
                List(0, 0, 1)
            )
        val empty = List()
        // 打印数据
        println(fruit, nums, diag3, empty)
    }

    @Test
    def testList1(): Unit = {
        // List(apples, oranges, pears)
        val fruit = "apples" :: ("oranges" :: ("pears" :: Nil))
        // List(1, 2, 3, 4)
        val nums = 1 :: (2 :: (3 :: (4 :: Nil)))
        // List(List(1, 0, 0), List(0, 1, 0), List(0, 0, 1))
        val diag3 = (1 :: (0 :: (0 :: Nil))) ::
          (0 :: (1 :: (0 :: Nil))) ::
          (0 :: (0 :: (1 :: Nil))) :: Nil
        // List()
        val empty = Nil

        println(fruit, nums, diag3, empty)

        val List(a, b, c) = fruit
        // apples, oranges, pears
        println(a, b, c)

        // rest 匹配长度大于等于2的列表
        val a1 :: b1 :: rest = fruit
        println(a1, b1, rest)
    }

    @Test
    def testFirstOrderMethods(): Unit = {
        // ::: 接收两个列表参数， :: 可以接收字面量
        println(List(1, 2) ::: List(3, 4, 5))
        println(2 :: List(1, 2))
        println(2 :: 1 :: Nil)
        // List(List(1, 2), 3, 4, 5)
        println(List(1, 2) :: List(3, 4, 5))

        val abcde = List('a', 'b', 'c', 'd', 'e')
        // List(a, b, c, d, e)
        println(abcde)
        // e
        println(abcde.last)
        // List(e, d, c, b, a)
        println(abcde.reverse)
        // List(a, b)
        println(abcde take 2)
        // List(c, d, e)
        println(abcde drop 2)
        // (List(a, b),List(c, d, e))
        println(abcde splitAt 2)
        // c
        println(abcde apply 2)
        // c
        println(abcde(2))
        // Range 0 until 5
        println(abcde.indices)

        // [a,b,c,d,e]
        println(abcde mkString("[", ",", "]"))
        // a b c d e
        println(abcde mkString " ")
        // List(a, b, c, d, e)
        println(abcde mkString("List(", ", ", ")"))

        // (a, b, c, d, e)
        val buf = new StringBuilder
        println(abcde addString(buf, "(", ", ", ")"))
    }

    def msort[T](less: (T, T) => Boolean)
                (xs: List[T]): List[T] = {

        def merge(xs: List[T], ys: List[T]): List[T] =
            (xs, ys) match {
                case (Nil, _) => ys
                case (_, Nil) => xs
                case (x :: xs1, y :: ys1) =>
                    if (less(x, y)) x :: merge(xs1, ys)
                    else y :: merge(xs, ys1)
            }

        val n = xs.length / 2
        if (n == 0) xs
        else {
            val (ys, zs) = xs splitAt n
            merge(msort(less)(ys), msort(less)(zs))
        }
    }

    @Test
    def testSort(): Unit = {
        // List(1, 3, 5, 7)
        println(msort((x: Int, y: Int) => x < y)(List(5, 7, 1, 3)))
    }

    @Test
    def testHigherOrderMethods(): Unit = {
        // List(2, 3, 4)
        println(List(1, 2, 3) map (_ + 1))

        val words = List("the", "quick", "brown", "fox")
        // List(3, 5, 5, 3)
        println(words map (_.length))

        // List(List(t, h, e), List(q, u, i, c, k), List(b, r, o, w, n), List(f, o, x))
        println(words map (_.toList))

        // List(t, h, e, q, u, i, c, k, b, r, o, w, n, f, o, x)
        println(words flatMap (_.toList))

        // List(eht, kciuq, nworb, xof)
        println(words map (_.toList.reverse.mkString))

        // List((2,1), (3,1), (3,2), (4,1), (4,2), (4,3))
        println(List.range(1, 5) flatMap (i => List.range(1, i) map (j => (i, j))))
        // List((2,1), (3,1), (3,2), (4,1), (4,2), (4,3))
        println(for (i <- List.range(1, 5); j <- List.range(1, i)) yield (i, j))
    }

    @Test
    def testFilterList(): Unit = {
        // List(2, 4)
        println(List(1, 2, 3, 4, 5) filter (_ % 2 == 0))

        // List(the, fox)
        val words = List("the", "quick", "brown", "fox")
        println(words filter (_.length == 3))

        // (List(2, 4),List(1, 3, 5))
        println(List(1, 2, 3, 4, 5) partition (_ % 2 == 0))

        // Some(2)
        println(List(1, 2, 3, 4, 5) find (_ % 2 == 0))

        // List(quick, brown, fox)
        println(words dropWhile (_ startsWith "t"))

        def hasZeroRow(m: List[List[Int]]) = m exists (row => row forall (_ == 0))

        val diag3 = List(List(1, 0, 0), List(0, 1, 0), List(0, 0, 1))
        // false
        println(hasZeroRow(diag3))

        //  the quick brown fox  最前有空格
        println(("" /: words) (_ + " " + _))
        // the quick brown fox
        println((words.head /: words.tail) (_ + " " + _))

        // List(-3, 1, 2, 4, 6)
        println(List(1, -3, 4, 2, 6) sortWith (_ < _))
        // List(quick, brown, the, fox)
        println(words sortWith (_.length > _.length))
    }

    @Test
    def testListObjectMethods(): Unit = {
        // List(1, 2, 3)
        println(List.apply(1, 2, 3))

        // List(1, 2, 3, 4)
        println(List.range(1, 5))

        // List(a, a, a, a, a)
        println(List.fill(5)('a'))

        // List(hello, hello, hello)
        println(List.fill(3)("hello"))

        // List(List(b, b, b), List(b, b, b))
        println(List.fill(2, 3)('b'))

        // List(0, 1, 4, 9, 16)
        println(List.tabulate(5)(n => n * n))

        // List(List(0, 0, 0, 0, 0), List(0, 1, 2, 3, 4), List(0, 2, 4, 6, 8), List(0, 3, 6, 9, 12), List(0, 4, 8, 12, 16))
        println(List.tabulate(5, 5)(_ * _))

        // List(a, b, c)
        println(List.concat(List('a', 'b'), List('c')))

        // List(b, c)
        println(List.concat(List(), List('b'), List('c')))

        // List()
        println(List.concat())
    }

    @Test
    def testMultipleListsTogether(): Unit = {
        // List(30, 80)
        println((List(10, 20), List(3, 4, 5)).zipped.map(_ * _))

        // true
        println((List("abc", "de"), List(3, 2)).zipped.forall(_.length == _))

        // false
        println((List("abc", "de"), List(3, 2)).zipped.exists(_.length != _))
    }

    @Test
    def testInferenceAlgorithm(): Unit = {
        // List(e, d, c, b, a)
        val abcde = List('a', 'b', 'c', 'd', 'e')
        println(msort((x: Char, y: Char) => x > y)(abcde))

        // List(e, d, c, b, a)
        println(abcde sortWith (_ > _))

        //
    }
}
