package com.hadoop.study.spark.core.req

import org.apache.spark.{SparkConf, SparkContext}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/26 16:26
 */

object UserVisitTop10Example {

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
        val clickCategoryRDD = actionRDD.filter(action => action.clickCategoryId != -1)
          .map(action => (action.clickCategoryId, 1))
          .reduceByKey(_ + _)

        val orderCategoryRDD = actionRDD.filter(action => action.orderCategoryIds != "null")
          .flatMap(action => {
              val categoryIds = action.orderCategoryIds.split(",")
              categoryIds.map(categoryId => (categoryId.toLong, 1))
          }).reduceByKey(_ + _)

        val payCategoryRDD = actionRDD.filter(action => action.payCategoryIds != "null")
          .flatMap(action => {
              val categoryIds = action.payCategoryIds.split(",")
              categoryIds.map(categoryId => (categoryId.toLong, 1))
          }).reduceByKey(_ + _)

        // 5. 合并 connect + group
        val categoryRDD = clickCategoryRDD.cogroup(orderCategoryRDD, payCategoryRDD)

        // 6. 转换结构
        val categoryRDD2 = categoryRDD.mapValues {
            case (clickIter, orderIter, payIter) =>
                // 点击数量
                var clickCount = 0
                if (clickIter.iterator.hasNext) {
                    clickCount = clickIter.iterator.next()
                }
                // 订单数量
                var orderCount = 0
                if (orderIter.iterator.hasNext) {
                    orderCount = orderIter.iterator.next()
                }
                // 支付数量
                var payCount = 0
                if (payIter.iterator.hasNext) {
                    payCount = payIter.iterator.next()
                }
                (clickCount, orderCount, payCount)
            case _ => (0, 0, 0)
        }

        // 7. 排序，取前10
        val tupleRDD = categoryRDD2.sortBy(_._2, ascending = false).take(10)
        tupleRDD.foreach(println)

        // 8. 停止
        sc.stop()
    }

    /**
     * 构造函数
     *
     * @param time             日期
     * @param userId           用户ID
     * @param sessionId        Session ID
     * @param pageId           页面ID
     * @param actionTime       动作时间
     * @param search           搜索关键字
     * @param clickCategoryId  点击的品类ID
     * @param clickProductId   点击的产品ID
     * @param orderCategoryIds 下单的品类ID
     * @param orderProductIds  下单的产品ID
     * @param payCategoryIds   支付的品类ID
     * @param payProductIds    支付的产品ID
     * @param cityId           城市ID
     */
    case class UserAction(time: String,
                          userId: Long,
                          sessionId: String,
                          pageId: Long,
                          actionTime: String,
                          search: String,
                          clickCategoryId: Long,
                          clickProductId: Long,
                          orderCategoryIds: String,
                          orderProductIds: String,
                          payCategoryIds: String,
                          payProductIds: String,
                          cityId: Long)
}
