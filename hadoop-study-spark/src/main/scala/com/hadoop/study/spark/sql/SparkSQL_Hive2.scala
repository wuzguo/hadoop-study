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
        System.setProperty("HADOOP_USER_NAME", "root")

        //  创建SparkSQL的运行环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_Hive2")
        val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

        spark.sql("use spark-sql")
        
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
              |               a.*,
              |               p.product_name,
              |               c.area,
              |               c.city_name
              |            from user_visit_action a
              |            join product_info p on a.click_product_id = p.product_id
              |            join city_info c on a.city_id = c.city_id
              |            where a.click_product_id > -1
              |        ) t1 group by area, product_name
              |    ) t2
              |) t3 where rank <= 3
            """.stripMargin).show

        // 2. 关闭环境
        spark.close()
    }
}
