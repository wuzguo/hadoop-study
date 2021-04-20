package com.hadoop.study.scala.chapter29.abstraction

import com.hadoop.study.scala.chapter29.recipe.{Food, Recipe}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/20 20:38
 */

abstract class Database {

    def allFoods: List[Food]
    def allRecipes: List[Recipe]

    def foodNamed(name: String) =
        allFoods.find(f => f.name == name)

    case class FoodCategory(name: String, foods: List[Food])
    def allCategories: List[FoodCategory]
}