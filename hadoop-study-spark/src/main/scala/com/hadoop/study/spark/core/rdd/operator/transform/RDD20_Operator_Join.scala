package com.hadoop.study.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD20_Operator_Join {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - join (Key - Value类型)
        // 在类型为(K,V)和(K,W)的RDD上调用，返回一个相同key对应的所有元素连接在一起的(K,(V,W))的RDD
        val rdd1 = sc.makeRDD(List(
            ("a", 1), ("a", 2), ("c", 3)
        ))

        val rdd2 = sc.makeRDD(List(
            ("a", 5), ("c", 6), ("a", 4), ("b", 4)
        ))

        // join : 两个不同数据源的数据，相同的key的value会连接在一起，形成元组
        //        如果两个数据源中key没有匹配上，那么数据不会出现在结果中
        //        如果两个数据源中key有多个相同的，会依次匹配，可能会出现笛卡尔乘积，数据量会几何性增长，会导致性能降低。
        val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

        joinRDD.collect().foreach(println)
        sc.stop()
    }
}
