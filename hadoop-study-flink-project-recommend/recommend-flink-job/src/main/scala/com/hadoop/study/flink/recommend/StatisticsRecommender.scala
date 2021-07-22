package com.hadoop.study.flink.recommend

import com.hadoop.study.flink.recommend.beans.{RateProducts, Rating}
import com.hadoop.study.flink.recommend.config.MongoConfig
import com.hadoop.study.flink.recommend.sinks.MongoSink
import com.hadoop.study.flink.recommend.sources.RatingMongoSource
import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/22 15:23
 */

object StatisticsRecommender {

    def main(args: Array[String]): Unit = {
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val benv = BatchTableEnvironment.create(env)
        val dataSet = env.createInput[Rating](RatingMongoSource("recommender", "ratings"))
        benv.createTemporaryView("ratings", dataSet)
        historyHotProducts(benv)
        env.execute("statistics recommender")
    }

    // 历史热门
    def historyHotProducts(benv: BatchTableEnvironment): Unit = {
        val sql2 = " SELECT * , ROW_NUMBER() OVER (PARTITION BY productId ORDER BY hot DESC) as rowNumber FROM (SELECT productId, COUNT(productId) as hot FROM ratings GROUP BY productId ORDER BY hot DESC)"
        // 只保存前 100 热门数据
       // val sql = "select productId, count(productId) as hot from ratings group by productId order by hot desc
       // limit 100"

        val sql = "select * from ratings "
        val table = benv.sqlQuery(sql)
        table.printSchema()
        val result = benv.toDataSet[RateProducts](table)
        result.print

        val mongoConfig = MongoConfig("mongodb://localhost:27017/recommender", "recommender")
        result.output(MongoSink[RateProducts]("recommender", "rate_products"))
    }
}
