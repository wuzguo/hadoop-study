package com.hadoop.study.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/28 15:49
 */

object SparkSQL_JDBC {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_JDBC")
        val spark = SparkSession.builder().config(conf).getOrCreate()

        // 连接数据库
        val properties = new Properties()
        properties.setProperty("driver", "com.mysql.cj.jdbc.Driver")
        properties.setProperty("user", "root")
        properties.setProperty("password", "123456")
        val df = spark.read.jdbc("jdbc:mysql://localhost:3306/metastore", "columns_v2", properties)

        // 1. 显示结果
        df.show()

        // 2. 查询 DSL
        df.createOrReplaceTempView("columns")
        df.select("CD_ID", "COLUMN_NAME", "TYPE_NAME", "INTEGER_IDX").show()

        // 3. SQL
        spark.sql("select * from columns where CD_ID = 22").show()

        // 关闭
        spark.stop()
    }
}
