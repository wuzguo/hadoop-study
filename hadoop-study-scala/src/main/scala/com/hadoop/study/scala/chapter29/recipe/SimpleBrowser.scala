package com.hadoop.study.scala.chapter29.recipe

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/20 20:32
 */

object SimpleBrowser {
    def recipesUsing(food: Food) =
        SimpleDatabase.allRecipes.filter(recipe =>
            recipe.ingredients.contains(food))

    def displayCategory(category: SimpleDatabase.FoodCategory) = {
        println(category)
    }
}