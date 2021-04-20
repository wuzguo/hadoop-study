package com.hadoop.study.scala.chapter29.abstraction

import com.hadoop.study.scala.chapter29.recipe.{Apple, Cream, FruitSalad, Orange, Recipe, Sugar}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/20 20:38
 */

object SimpleDatabase2 extends Database {

    def allFoods = List(Apple, Orange, Cream, Sugar)

    def allRecipes: List[Recipe] = List(FruitSalad)

    private var categories = List(
        FoodCategory("fruits", List(Apple, Orange)),
        FoodCategory("misc", List(Cream, Sugar)))

    def allCategories = categories
}