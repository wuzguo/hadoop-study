package com.hadoop.study.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD11_Operator_Repartition {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - repartition
        // 该操作内部其实执行的是coalesce操作，参数shuffle的默认值为true
        val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

        // coalesce算子可以扩大分区的，但是如果不进行shuffle操作，是没有意义，不起作用。
        // 所以如果想要实现扩大分区的效果，需要使用shuffle操作
        // spark提供了一个简化的操作
        // 缩减分区：coalesce，如果想要数据均衡，可以采用shuffle
        // 扩大分区：repartition, 底层代码调用的就是coalesce，而且肯定采用shuffle
        //val newRDD: RDD[Int] = rdd.coalesce(3, true)
        val newRDD: RDD[Int] = rdd.repartition(3)

        newRDD.saveAsTextFile("./hadoop-study-datas/spark/output")
        sc.stop()
    }
}
