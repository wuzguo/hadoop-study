package com.hadoop.study.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/25 16:57
 */

object RDD23_Operator_Agent_Example {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("Agent")
        val sc = new SparkContext(conf)

        // 读取数据. 并且分割
        val dataAgent = sc.textFile("./hadoop-study-datas/spark/data/agent.txt")

        // 转换为((城市，广告), 1)
        val mapCityAds = dataAgent.map(lines => {
            val values = lines.split(" ")
            ((values(1), values(4)), 1)
        })

        // 聚合 ((城市，广告), 1) => ((城市，广告), sum)
        val mapReduce = mapCityAds.reduceByKey(_ + _)

        // 结构转换 ((城市，广告), sum) => (城市，(广告, sum))
        val mapCityValue = mapReduce.map {
            case ((city, ad), sum) => (city, (ad, sum))
        }.sortBy(_._1)

        // 组合 (城市，(广告, sum)) => (城市，((广告1, sum), (广告2, sum)))
        val groupRDD = mapCityValue.groupByKey()

        // 排序，取前3
        val mapValues = groupRDD.mapValues(iter => {
            iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
        })

        // 打印结果
        mapValues.collect().sortBy(_._1).foreach(println)

        sc.stop()
    }
}
