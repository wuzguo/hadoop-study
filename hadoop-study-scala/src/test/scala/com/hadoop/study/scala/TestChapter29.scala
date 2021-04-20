package com.hadoop.study.scala

import com.hadoop.study.scala.chapter29.abstraction.{SimpleBrowser2, SimpleDatabase2, StudentBrowser, StudentDatabase}
import com.hadoop.study.scala.chapter29.recipe.{SimpleBrowser, SimpleDatabase}
import org.junit.{FixMethodOrder, Test}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/20 20:33
 */

@FixMethodOrder
class TestChapter29 {

    @Test
    def testRecipe(): Unit = {
        val apple = SimpleDatabase.foodNamed("Apple").get
        println(apple)
        println(SimpleBrowser.recipesUsing(apple))
    }

    @Test
    def testAbstractionRecipe(): Unit = {
        val apple = SimpleDatabase2.foodNamed("Apple").get
        println(apple)
        println(SimpleBrowser2.recipesUsing(apple))
    }

    @Test
    def testStudentRecipe(): Unit = {
        val apple = StudentDatabase.foodNamed("Apple").get
        println(apple)
        println(StudentBrowser.recipesUsing(apple))
    }
}
