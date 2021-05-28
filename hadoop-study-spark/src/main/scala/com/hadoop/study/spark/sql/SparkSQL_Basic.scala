package com.hadoop.study.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/28 14:07
 */

object SparkSQL_Basic {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("sparkSQL").setMaster("local[*]")
        val spark = SparkSession.builder().config(conf).getOrCreate()
        val frame = spark.read.json("./hadoop-study-datas/spark/sql/user.json")
        frame.show()
        spark.stop()
    }
}
