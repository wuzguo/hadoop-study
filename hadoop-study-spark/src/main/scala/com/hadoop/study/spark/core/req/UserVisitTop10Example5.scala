package com.hadoop.study.spark.core.req

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/27 9:59
 */

object UserVisitTop10Example5 {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("UserVisitTop10Example5")
        val sc = new SparkContext(conf)

        // 1. 读取文件
        val fileRDD = sc.textFile("./hadoop-study-datas/spark/core/user_visit_action.txt")
        fileRDD.cache()

        // 2. 页面跳转顺序定义, 取前10的页面
        val pages = fileRDD.flatMap(lines => {
            val datas = lines.split("_")
            List(datas(3).toInt)
        }).distinct().sortBy(page => page).take(100)

        // 页面链条
        val pageZip = pages.zip(pages.tail)

        // 3. 注册累加器，统计每个页面的访问数量
        val pageAccumulator = new PageAccumulator
        sc.register(pageAccumulator, "pageAccumulator")

        // 4. 遍历（sessionId, pageId）
        val groupRDD = fileRDD.flatMap(lines => {
            val datas = lines.split("_")
            val pageId = datas(3).toInt
            pageAccumulator.add(pageId)
            List((datas(2), (pageId, datas(4))))
        }).groupByKey()

        // 5. 拉链拉一下，累加器计数
        val pageZipRDD = groupRDD.map {
            case (_, pageIter) =>
                val pageIds = pageIter.toList.sortBy(f => f._2).map(f => f._1)
                val pageIdZip = pageIds.zip(pageIds.tail)
                pageIdZip.filter(f => pageZip.contains(f)).map(f => {
                    (f, 1)
                })
        }

        // 6. 压扁
        val pageFlatRDD = pageZipRDD.flatMap(f => f).reduceByKey(_ + _)

        // 7. 计算，广播
        val mapPage = sc.broadcast(pageAccumulator.value)
        val value = pageFlatRDD.map {
            case ((pre, next), sum) =>
                val count = mapPage.value.getOrElse(pre, 0)
                ((pre, next), "%s%%".format((sum.toDouble * 100 / count).formatted("%.2f")), count, sum)
        }

        // 8. 打印
        value.sortBy(f => f._2, ascending = false).collect()
          .foreach(f => println(s"页面（${f._1._1} => ${f._1._2}）， 跳转率：${f._2}， 访问数量： ${f._3} , ${f._4}"))

        // 9. 关闭
        sc.stop()
    }

    // 页面数
    class PageAccumulator extends AccumulatorV2[Int, mutable.Map[Int, Int]] {

        private val mapPages = mutable.Map[Int, Int]()

        override def isZero: Boolean = mapPages.isEmpty

        override def copy(): AccumulatorV2[Int, mutable.Map[Int, Int]] = new PageAccumulator

        override def reset(): Unit = mapPages.clear()

        override def add(v: Int): Unit = {
            var count = mapPages.getOrElse(v, 0)
            count += 1
            mapPages.put(v, count)
        }

        override def merge(other: AccumulatorV2[Int, mutable.Map[Int, Int]]): Unit = {
            other.value.foreach {
                case (page, count) =>
                    var i = mapPages.getOrElse(page, 0)
                    i += count
                    mapPages.put(page, i)
            }
        }

        override def value: mutable.Map[Int, Int] = mapPages
    }
}
