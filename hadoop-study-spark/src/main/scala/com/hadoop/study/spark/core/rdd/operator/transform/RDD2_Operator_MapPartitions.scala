package com.hadoop.study.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD2_Operator_MapPartitions {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - mapPartitions
        // 将待处理的数据以分区为单位发送到计算节点进行处理，这里的处理是指可以进行任意的处理，哪怕是过滤数据。
        val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

        // mapPartitions : 可以以分区为单位进行数据转换操作
        //                 但是会将整个分区的数据加载到内存进行引用
        //                 如果处理完的数据是不会被释放掉，存在对象的引用。
        //                 在内存较小，数据量较大的场合下，容易出现内存溢出。
        val mpRDD: RDD[Int] = rdd.mapPartitions(
            iter => {
                println(">>>>>>>>>>")
                iter.map(_ * 2)
            }
        )
        mpRDD.collect().foreach(println)

        sc.stop()

    }
}
