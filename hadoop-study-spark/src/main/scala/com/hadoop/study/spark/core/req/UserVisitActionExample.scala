package com.hadoop.study.spark.core.req

import org.apache.spark.{SparkConf, SparkContext}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/26 16:26
 */

object UserVisitActionExample {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("UserActionAnalysis")
        val sc = new SparkContext(conf)

        // 1. 读取文件
        val fileRdd = sc.textFile("./hadoop-study-datas/spark/core/user_visit_action.txt")
        // 2. 转换为对象
        val actionRDD = fileRdd.map(f => {
            val actions = f.split("_")

            UserAction(actions(0).trim,
                actions(1).toLong,
                actions(2).trim,
                actions(3).toLong,
                actions(4).trim,
                actions(5).trim,
                actions(6).toLong,
                actions(7).toLong,
                actions(8).trim,
                actions(9).trim,
                actions(10).trim,
                actions(11).trim,
                actions(12).toLong)
        })

        // 3. 缓存
        actionRDD.cache()

        // 4. 统计每个品类点击的次数、下单的次数和支付的次数
        val clickCategoryRDD = actionRDD.filter(action => action.clickCategoryId != -1).map(action => (action
          .clickCategoryId, 1))

        val orderCategoryRDD = actionRDD.filter(action => action.orderCategoryIds != "null").flatMap(action => {
            val categoryIds = action.orderCategoryIds.split(",")
            categoryIds.map(categoryId => (categoryId.toLong, 1))
        })

        val payCategoryRDD = actionRDD.filter(action => action.payCategoryIds != "null").flatMap(action => {
            val categoryIds = action.payCategoryIds.split(",")
            categoryIds.map(categoryId => (categoryId.toLong, 1))
        })
    }

    case class UserAction(time: String, // 日期
                          userId: Long, // 用户ID
                          sessionId: String, // Session ID
                          pageId: Long, // 页面ID
                          actionTime: String, // 动作时间
                          search: String, // 搜索关键字
                          clickCategoryId: Long, // 点击的品类ID
                          clickProductId: Long, // 点击的产品ID
                          orderCategoryIds: String, // 下单的品类ID
                          orderProductIds: String, // 下单的产品ID
                          payCategoryIds: String, // 支付的品类ID
                          payProductIds: String, // 支付的产品ID
                          cityId: Long) // 城市ID
}
