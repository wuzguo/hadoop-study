package com.hadoop.study.scala.chapter29.splitting

import com.hadoop.study.scala.chapter29.recipe.{Food, Recipe}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/20 20:55
 */

abstract class Database3 extends FoodCategories {

    def allFoods: List[Food]

    def allRecipes: List[Recipe]

    def foodNamed(name: String) =
        allFoods.find(f => f.name == name)
}