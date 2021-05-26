package com.hadoop.study.spark.core.req

import org.apache.spark.{SparkConf, SparkContext}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/26 16:26
 */

object UserVisitActionExample2 {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("UserActionAnalysis")
        val sc = new SparkContext(conf)

        // 1. 读取文件
        val fileRdd = sc.textFile("./hadoop-study-datas/spark/core/user_visit_action.txt")
        // 2. 转换为对象
        val flatRDD = fileRdd.flatMap(lines => {
            val datas = lines.split("_")
            // 直接转换为 （类别， （点击数量，下单数量，支付数量））
            if (datas(6) != "-1") {
                List((datas(6), (1, 0, 0)))
            } else if (datas(8) != "null") {
                datas(8).trim.split(",").map(category => (category, (0, 1, 0)))
            } else if (datas(10) != "null") {
                datas(10).trim.split(",").map(category => (category, (0, 0, 1)))
            } else {
                Nil
            }
        })

        // 3. 缓存
        flatRDD.cache()

        // 4. 将相同的品类ID的数据进行分组聚合 ( 品类ID，( 点击数量, 下单数量, 支付数量 ) )
        val analysisRDD = flatRDD.reduceByKey(
            (value1, value2) => {
                (value1._1 + value2._1, value1._2 + value2._2, value1._3 + value2._3)
            }
        )

        // 5. 排序，取前10
        val tupleRDD = analysisRDD.sortBy(_._2, ascending = false).take(10)
        tupleRDD.foreach(println)

        // 6. 停止
        sc.stop()
    }
}
