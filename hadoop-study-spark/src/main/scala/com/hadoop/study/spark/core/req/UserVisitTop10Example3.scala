package com.hadoop.study.spark.core.req

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/26 16:26
 */

object UserVisitTop10Example3 {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("UserVisitTop10Example3")
        val sc = new SparkContext(conf)

        // 1. 读取文件
        val fileRdd = sc.textFile("./hadoop-study-datas/spark/core/user_visit_action.txt")

        // 2. 注册累加器
        val categoryAccumulator = new CategoryAccumulator
        sc.register(categoryAccumulator, "categoryAccumulator")

        // 3. 转换为对象
        fileRdd.foreach(lines => {
            val datas = lines.split("_")
            // 直接转换为 （类别， （点击数量，下单数量，支付数量））
            if (datas(6) != "-1") {
                categoryAccumulator.add((datas(6).toLong, 0))
            } else if (datas(8) != "null") {
                datas(8).trim.split(",").map(category => categoryAccumulator.add((category.toLong, 1)))
            } else if (datas(10) != "null") {
                datas(10).trim.split(",").map(category => categoryAccumulator.add((category.toLong, 2)))
            } else {
            }
        })

        // 4. 获取计算结果
        val categoryValue = categoryAccumulator.value

        // 5. 排序，取前10
        val categories = categoryValue.values.toList.sortWith((left, right) => {
            if (left.clickCount > right.clickCount) {
                true
            } else if (left.clickCount == right.clickCount) {
                if (left.orderCount > right.orderCount) {
                    true
                } else if (left.orderCount == right.orderCount) {
                    if (left.payCount > right.payCount) {
                        true
                    } else if (left.payCount == right.payCount) {
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            }
        }).take(10)

        // 6. 打印
        categories.foreach(println)

        // 7. 停止
        sc.stop()
    }


    case class HotCategory(categoryId: Long, var clickCount: Long, var orderCount: Long, var payCount: Long) {
        override def toString: String = s"category: $categoryId" + s": click $clickCount" + s"，order $orderCount" +
          s"，pay $payCount"
    }


    class CategoryAccumulator extends AccumulatorV2[(Long, Int), mutable.Map[Long, HotCategory]] {
        // Map
        private val categoryMap = mutable.Map[Long, HotCategory]()

        override def isZero: Boolean = categoryMap.isEmpty

        override def copy(): AccumulatorV2[(Long, Int), mutable.Map[Long, HotCategory]] = new CategoryAccumulator

        override def reset(): Unit = categoryMap.clear

        override def merge(other: AccumulatorV2[(Long, Int), mutable.Map[Long, HotCategory]]): Unit = {
            other.value.foreach {
                case (key, hotCategory) =>
                    val category = categoryMap.getOrElse(key, HotCategory(key, 0, 0, 0))
                    category.clickCount = category.clickCount + hotCategory.clickCount
                    category.orderCount = category.orderCount + hotCategory.orderCount
                    category.payCount = category.payCount + hotCategory.payCount
                    categoryMap.update(key, category)
            }

        }

        override def value: mutable.Map[Long, HotCategory] = categoryMap

        override def add(value: (Long, Int)): Unit = {
            val category = categoryMap.getOrElse(value._1, HotCategory(value._1, 0, 0, 0))
            if (value._2 == 0) {
                category.clickCount += 1
            } else if (value._2 == 1) {
                category.orderCount += 1
            } else if (value._2 == 2) {
                category.payCount += 1
            } else {
            }
            categoryMap.update(value._1, category)
        }
    }
}
