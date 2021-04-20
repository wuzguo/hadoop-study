package com.hadoop.study.scala.chapter29.abstraction

import com.hadoop.study.scala.chapter29.recipe.{Food, Recipe}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/20 20:52
 */

object StudentDatabase extends Database {

    object FrozenFood extends Food("FrozenFood")

    object HeatItUp extends Recipe(
        "heat it up",
        List(FrozenFood),
        "Microwave the 'food' for 10 minutes.")

    def allFoods = List(FrozenFood)

    def allRecipes = List(HeatItUp)

    def allCategories = List(
        FoodCategory("edible", List(FrozenFood)))
}
