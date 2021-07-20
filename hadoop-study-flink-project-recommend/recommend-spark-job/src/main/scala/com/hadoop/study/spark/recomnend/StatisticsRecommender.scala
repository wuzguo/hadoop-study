package com.hadoop.study.spark.recomnend

import com.hadoop.study.spark.recomnend.beans.Rating
import com.hadoop.study.spark.recomnend.config.MongoConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/20 15:44
 */

object StatisticsRecommender {

    // 定义mongodb中存储的表名
    val MONGODB_RATING_COLLECTION = "ratings"

    val RATE_MORE_PRODUCTS = "rate_products"

    val RATE_MORE_RECENTLY_PRODUCTS = "rate_recently_products"

    val AVERAGE_PRODUCTS = "average_products"

    def main(args: Array[String]): Unit = {
        val config = Map(
            "spark.cores" -> "local[1]",
            "mongo.uri" -> "mongodb://localhost:27017/recommender",
            "mongo.db" -> "recommender"
        )
        // 创建一个spark config
        val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")
        // 创建spark session
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        import spark.implicits._
        implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

        // 加载数据
        val ratingDF = spark.read
          .option("uri", mongoConfig.uri)
          .option("collection", MONGODB_RATING_COLLECTION)
          .format("com.mongodb.spark.sql")
          .load()
          .as[Rating]
          .toDF()

        // 创建一张叫ratings的临时表
        ratingDF.createOrReplaceTempView("ratings")

        // TODO: 用spark sql去做不同的统计推荐
        // 1. 历史热门商品，按照评分个数统计，productId，count
        val rateProductsDF = spark.sql("select productId, count(productId) as count from ratings group by productId order by count desc")
        store(rateProductsDF, RATE_MORE_PRODUCTS)

        // 2. 近期热门商品，把时间戳转换成yyyyMM格式进行评分个数统计，最终得到productId, count, yearmonth
        // 创建一个日期格式化工具
        val dateFormat = new SimpleDateFormat("yyyyMM")
        // 注册UDF，将timestamp转化为年月格式yyyyMM
        spark.udf.register("changeDate", (timestamp: Int) => dateFormat.format(new Date(timestamp * 1000L)).toInt)
        // 把原始rating数据转换成想要的结构productId, score, yearmonth
        val ratingOfYearMonthDF = spark.sql("select productId, score, changeDate(timestamp) as yearmonth from ratings")
        ratingOfYearMonthDF.createOrReplaceTempView("ratingOfMonth")
        val rateRecentlyProductsDF = spark.sql("select productId, count(productId) as count, yearmonth from ratingOfMonth group by yearmonth, productId order by yearmonth desc, count desc")
        // 把df保存到mongodb
        store(rateRecentlyProductsDF, RATE_MORE_RECENTLY_PRODUCTS)

        // 3. 优质商品统计，商品的平均评分，productId，avg
        val averageProductsDF = spark.sql("select productId, avg(score) as avg from ratings group by productId order by avg desc")
        store(averageProductsDF, AVERAGE_PRODUCTS)

        // 停止
        spark.stop()
    }

    def store(df: DataFrame, colName: String)(implicit mongoConfig: MongoConfig): Unit = {
        df.write
          .option("uri", mongoConfig.uri)
          .option("collection", colName)
          .mode("overwrite")
          .format("com.mongodb.spark.sql")
          .save()
    }
}
