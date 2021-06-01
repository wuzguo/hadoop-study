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

object SparkSQL_Hive2 {

    def main(args: Array[String]): Unit = {
        // 设置环境变量
        System.setProperty("HADOOP_USER_NAME", "zak")

        //  创建SparkSQL的运行环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_Hive2")
        val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

        spark.sql("use spark_sql")
        
        // 准备数据
        spark.sql(
            """
              |select
              |    *
              |from (
              |    select
              |        *,
              |        rank() over( partition by area order by clickCnt desc ) as rank
              |    from (
              |        select
              |           area,
              |           product_name,
              |           count(*) as clickCnt
              |        from (
              |            select
              |               action.*,
              |               info.product_name,
              |               city.area,
              |               city.city_name
              |            from user_visit_action action
              |            join product_info info on action.click_product_id = info.product_id
              |            join city_info city on action.city_id = city.city_id
              |            where action.click_product_id > -1
              |        ) t1 group by area, product_name
              |    ) t2
              |) t3 where rank <= 3
            """.stripMargin).show

        // 2. 关闭环境
        spark.close()
    }
}
