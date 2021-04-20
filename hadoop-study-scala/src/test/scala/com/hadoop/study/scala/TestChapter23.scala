package com.hadoop.study.scala

import com.hadoop.study.scala.chapter23.{Book, Person}
import org.junit.{FixMethodOrder, Test}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/20 15:56
 */

@FixMethodOrder
class TestChapter23 {

    @Test
    def testFor(): Unit = {

        val lara = Person("Lara", false)
        val bob = Person("Bob", true)
        val julie = Person("Julie", false, lara, bob)
        val persons = List(lara, bob, julie)

        val person2 = persons filter (p => !p.isMale) flatMap (p => p.children map (c => (p.name, c.name)))
        // List((Julie,Lara), (Julie,Bob))
        println(person2)

        val person3 = persons withFilter (p => !p.isMale) flatMap (p => p.children map (c => (p.name, c.name)))
        // List((Julie,Lara), (Julie,Bob))
        println(person3)

        val person4 = for (p <- persons; if !p.isMale; c <- p.children) yield (p.name, c.name)
        // List((Julie,Lara), (Julie,Bob))
        println(person4)
    }


    val books: List[Book] =
        List(
            Book(
                "Structure and Interpretation of Computer Programs",
                "Abelson, Harold", "Sussman, Gerald J."
            ),
            Book(
                "Principles of Compiler Design",
                "Aho, Alfred", "Ullman, Jeffrey"
            ),
            Book(
                "Programming in Modula-2",
                "Wirth, Niklaus"
            ),
            Book(
                "Elements of ML Programming",
                "Ullman, Jeffrey"
            ),
            Book(
                "The Java Language Specification", "Gosling, James",
                "Joy, Bill", "Steele, Guy", "Bracha, Gilad"
            )
        )


    def removeDuplicates[A](xs: List[A]): List[A] = {
        if (xs.isEmpty) xs else xs.head :: removeDuplicates(xs.tail filter (x => x != xs.head))
    }

    @Test
    def testBooks(): Unit = {
        val books2 = for (b <- books; a <- b.authors if a startsWith "Gosling") yield b.title
        // List(The Java Language Specification)
        println(books2)
        val books3 = for (b <- books if (b.title indexOf "Program") >= 0) yield b.title
        // List(Structure and Interpretation of Computer Programs, Programming in Modula-2, Elements of ML Programming)
        println(books3)
        val books4 = for (b1 <- books; b2 <- books if b1 != b2; a1 <- b1.authors; a2 <- b2.authors if a1 == a2) yield a1
        // List(Ullman, Jeffrey, Ullman, Jeffrey)
        println(books4)

        val books5 = removeDuplicates(books4)
        // List(Ullman, Jeffrey)
        println(books5)
    }
}
