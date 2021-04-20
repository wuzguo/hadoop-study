package com.hadoop.study.scala.chapter29.recipe

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/20 20:32
 */

object SimpleDatabase {

    def allFoods = List(Apple, Orange, Cream, Sugar)

    def foodNamed(name: String): Option[Food] =
        allFoods.find(_.name == name)

    def allRecipes: List[Recipe] = List(FruitSalad)

    case class FoodCategory(name: String, foods: List[Food])

    private var categories = List(
        FoodCategory("fruits", List(Apple, Orange)),
        FoodCategory("misc", List(Cream, Sugar)))

    def allCategories = categories
}

