package com.hadoop.study.spark.recomnend.loader

import com.hadoop.study.spark.recomnend.beans.{Product, Rating}
import com.hadoop.study.spark.recomnend.config.MongoConfig
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/20 14:35
 */

object DataLoader {

    def main(args: Array[String]): Unit = {
        // 配置文件
        val config = Map(
            "mongo.uri" -> "mongodb://localhost:27017/recommender",
            "mongo.db" -> "recommender"
        )
        // 创建一个spark config
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("DataLoader")
        // 创建spark session
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        import spark.implicits._
        implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

        // 加载数据
        val productRDD = spark.sparkContext.textFile("./hadoop-study-datas/project/data/products.csv")
        val productDF = productRDD.map(item => {
            // product数据通过^分隔，切分出来
            val attr = item.split("\\^")
            // 转换成Product
            Product(attr(0).toInt, attr(1).trim, attr(4).trim, attr(5).trim, attr(6).trim, System.currentTimeMillis())
        }).toDF()

        store(productDF, "Product", List("productId"))

        val ratingRDD = spark.sparkContext.textFile("./hadoop-study-datas/project/data/ratings.csv")
        val ratingDF = ratingRDD.map(item => {
            val attr = item.split(",")
            Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt, System.currentTimeMillis())
        }).toDF()

        store(ratingDF, "Rating", List("productId", "userId"))
        spark.stop()
    }

    def store(dataFrame: DataFrame, colName: String, indexNames: List[String])(implicit mongoConfig: MongoConfig): Unit = {
        // 新建一个mongodb的连接，客户端
        val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
        // 定义要操作的mongodb表，可以理解为 db.Product
        val collection = mongoClient(mongoConfig.db)(colName)

        // 如果表已经存在，则删掉
        collection.dropCollection()

        // 将当前数据存入对应的表中
        dataFrame.write
          .option("uri", mongoConfig.uri)
          .option("collection", colName)
          .mode("overwrite")
          .format("com.mongodb.spark.sql")
          .save()

        // 创建索引
        indexNames.foreach(indexName => collection.createIndex(MongoDBObject(indexName -> 1)))
        // 关闭
        mongoClient.close()
    }
}
