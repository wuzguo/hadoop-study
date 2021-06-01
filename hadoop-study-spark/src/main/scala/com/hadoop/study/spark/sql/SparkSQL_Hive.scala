package com.hadoop.study.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.io.File

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/28 16:35
 */

object SparkSQL_Hive {

    def main(args: Array[String]): Unit = {
        // 设置环境变量
        // System.setProperty("HADOOP_USER_NAME", "zak")

         val warehouseLocation = new File("/user/hive/warehouse").getAbsolutePath
        //  创建SparkSQL的运行环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_Hive")
          .set("spark.sql.warehouse.dir", warehouseLocation)
        val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

        // 使用SparkSQL连接外置的Hive
        // 1. 拷贝Hive-size.xml文件到classpath下
        // 2. 启用Hive的支持
        // 3. 增加对应的依赖关系（包含MySQL驱动）
        spark.sql("show databases").show
        spark.sql("use spark_sql")
        spark.sql("show tables").show
        // 2. 关闭环境
        spark.close()

        // 3. 异常处理
        // 如果出现： java.lang.UnsatisfiedLinkError: 'org.apache.hadoop.io.nativeio.NativeIO$POSIX$Stat
        // 删除本地Hadoop_Home/bin 目录下的 hadoop.dll 文件
    }
}
