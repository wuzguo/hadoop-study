package com.hadoop.study.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/27 9:17
 */

object UserVisitTop10Example4 {

    // Top 10 Session
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("UserVisitTop10Example4")
        val sc = new SparkContext(conf)

        // 1. 读取文件
        val fileRDD = sc.textFile("./hadoop-study-datas/spark/core/user_visit_action.txt")

        // 2. 计算TOP10类别
        val categories = categoryTop10(sc, fileRDD)

        // 3. 遍历获取Session
        val flatRDD = fileRDD.flatMap(lines => {
            val datas = lines.split("_")
            // 直接转换为 （类别， （点击数量，下单数量，支付数量））
            if (datas(6) != "-1" && categories.contains(datas(6).toLong)) {
                List(((datas(6).toLong, datas(2)), 1))
            } else if (datas(8) != "null") {
                datas(8).trim.split(",")
                  .filter(category => categories.contains(category.toLong))
                  .map(category => ((category.toLong, datas(2)), 1))
            } else if (datas(10) != "null") {
                datas(10).trim.split(",")
                  .filter(category => categories.contains(category.toLong))
                  .map(category => ((category.toLong, datas(2)), 1))
            } else {
                Nil
            }
        }).reduceByKey(_ + _)

        // 4. 转换结构
        val categoryRDD = flatRDD.map {
            case ((category, session), count) => (category, (session, count))
        }.groupByKey()

        // 5. 排序取前十
        val top10SessionRDD = categoryRDD.mapValues(iterable => {
            iterable.toList.sortBy(sec => sec._2)(Ordering.Int.reverse).take(10)
        })

        // 6. 打印
        top10SessionRDD.collect().foreach(println)

        // 7. 关闭
        sc.stop()
    }


    /**
     * 计算Top10 品类ID
     *
     * @param sc      SparkContext
     * @param fileRdd RDD
     * @return List
     */
    def categoryTop10(sc: SparkContext, fileRdd: RDD[String]): List[Long] = {
        // 1. 注册累加器
        val categoryAccumulator = new CategoryAccumulator
        sc.register(categoryAccumulator, "categoryAccumulator")

        // 2. 转换为对象
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

        // 3. 获取计算结果
        val categoryValue = categoryAccumulator.value

        // 4. 排序
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
        })

        // 5. 取前10
        categories.take(10).map(f => f.categoryId)
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
