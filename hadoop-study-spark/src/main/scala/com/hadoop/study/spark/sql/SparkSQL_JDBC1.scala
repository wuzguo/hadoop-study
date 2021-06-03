package com.hadoop.study.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/28 15:49
 */

object SparkSQL_JDBC1 {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_JDBC1")
        val spark = SparkSession.builder().config(conf).getOrCreate()

        // 读取文件
        val df = spark.read.json("./hadoop-study-datas/spark/sql/user.json")

        // 1. 写入数据库
        df.write.format("jdbc")
          .option("url", "jdbc:mysql://localhost:3306/spark-sql")
          .option("driver", "com.mysql.cj.jdbc.Driver")
          .option("user", "root")
          .option("password", "123456")
          .option("dbtable", "user")
          .mode(SaveMode.Append)
          .save()

        // 关闭
        spark.stop()
    }
}
