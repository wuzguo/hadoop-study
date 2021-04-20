package com.hadoop.study.scala.chapter29.abstraction

import com.hadoop.study.scala.chapter29.recipe.Food

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/20 20:37
 */

abstract class Browser {

    val database: Database

    def recipesUsing(food: Food) =
        database.allRecipes.filter(recipe =>
            recipe.ingredients.contains(food))

    def displayCategory(category: database.FoodCategory) = {
        println(category)
    }
}