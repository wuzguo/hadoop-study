package com.hadoop.study.scala.chapter29.splitting

import com.hadoop.study.scala.chapter29.recipe.Food

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/20 20:54
 */

trait FoodCategories {
    case class FoodCategory(name: String, foods: List[Food])

    def allCategories: List[FoodCategory]
}
