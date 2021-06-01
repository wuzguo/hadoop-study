package com.hadoop.study.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/28 16:35
 */

object SparkSQL_Hive1 {

    def main(args: Array[String]): Unit = {
        // 设置环境变量
         System.setProperty("HADOOP_USER_NAME", "zak")

        //  创建SparkSQL的运行环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_Hive1")
        val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

        spark.sql("use spark_sql")

        // 准备数据
        spark.sql(
            """
              |CREATE TABLE IF NOT EXISTS `user_visit_action` (
              |  `date` string,
              |  `user_id` bigint,
              |  `session_id` string,
              |  `page_id` bigint,
              |  `action_time` string,
              |  `search_keyword` string,
              |  `click_category_id` bigint,
              |  `click_product_id` bigint,
              |  `order_category_ids` string,
              |  `order_product_ids` string,
              |  `pay_category_ids` string,
              |  `pay_product_ids` string,
              |  `city_id` bigint)
              |row format delimited fields terminated by '\t'
            """.stripMargin)

        spark.sql(
            """
              |load data local inpath './hadoop-study-datas/spark/sql/user_visit_action.txt' into table spark_sql
              |.user_visit_action
            """.stripMargin)

        spark.sql(
            """
              |CREATE TABLE IF NOT EXISTS  `product_info` (
              |  `product_id` bigint,
              |  `product_name` string,
              |  `extend_info` string)
              |row format delimited fields terminated by '\t'
            """.stripMargin)

        spark.sql(
            """
              |load data local inpath './hadoop-study-datas/spark/sql/product_info.txt' into table spark_sql.product_info
            """.stripMargin)

        spark.sql(
            """
              |CREATE TABLE IF NOT EXISTS  `city_info` (
              |  `city_id` bigint,
              |  `city_name` string,
              |  `area` string)
              |row format delimited fields terminated by '\t'
            """.stripMargin)

        spark.sql(
            """
              |load data local inpath './hadoop-study-datas/spark/sql/city_info.txt' into table spark_sql.city_info
            """.stripMargin)

        spark.sql("""select * from city_info""").show

        // 2. 关闭环境
        spark.close()
    }
}
