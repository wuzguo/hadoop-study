package com.hadoop.study.scala.chapter29.splitting

import com.hadoop.study.scala.chapter29.recipe.{Apple, Recipe}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/20 20:56
 */

trait SimpleRecipes {

    object FruitSalad extends Recipe(
        "fruit salad",
        List(Apple, Pear),
        "Mix it all together."
    )

    def allRecipes = List(FruitSalad)
}