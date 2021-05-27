package com.hadoop.study.spark.core.req

import org.apache.spark.{SparkConf, SparkContext}

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
        

        sc.stop()
    }
}
