package com.hadoop.study.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD15_Operator_ReduceByKey {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - reduceByKey (Key - Value类型)
        // 可以将数据按照相同的Key对Value进行聚合
        val rdd = sc.makeRDD(List(
            ("a", 1), ("a", 2), ("a", 3), ("b", 4)
        ))

        // reduceByKey : 相同的key的数据进行value数据的聚合操作
        // scala语言中一般的聚合操作都是两两聚合，spark基于scala开发的，所以它的聚合也是两两聚合
        // 【1，2，3】
        // 【3，3】
        // 【6】
        // reduceByKey中如果key的数据只有一个，是不会参与运算的。
        val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey((x: Int, y: Int) => {
            println(s"x = ${x}, y = ${y}")
            x + y
        })

        reduceRDD.collect().foreach(println)
        sc.stop()
    }
}
