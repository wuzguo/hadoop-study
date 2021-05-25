package com.hadoop.study.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD10_Operator_Coalesce {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - coalesce
        // 根据数据量缩减分区，用于大数据集过滤后，提高小数据集的执行效率
        val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

        // coalesce方法默认情况下不会将分区的数据打乱重新组合
        // 这种情况下的缩减分区可能会导致数据不均衡，出现数据倾斜
        // 如果想要让数据均衡，可以进行shuffle处理
        //val newRDD: RDD[Int] = rdd.coalesce(2)
        val newRDD: RDD[Int] = rdd.coalesce(2, true)

        newRDD.saveAsTextFile("./hadoop-study-datas/spark/output")
        sc.stop()
    }
}
