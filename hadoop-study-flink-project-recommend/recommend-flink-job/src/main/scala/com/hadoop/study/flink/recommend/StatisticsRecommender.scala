package com.hadoop.study.flink.recommend

import com.hadoop.study.flink.recommend.beans.Rating
import com.hadoop.study.flink.recommend.sinks.{RateProductMongoSink, RateRecentlyProductMongoSink}
import com.hadoop.study.flink.recommend.sources.RatingMongoSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.FieldExpression
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}
import org.apache.flink.types.Row

import java.text.SimpleDateFormat
import java.util.Date

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/22 15:23
 */

object StatisticsRecommender {

    def main(args: Array[String]): Unit = {
        // 1、获取流式环境变量
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 2、添加自定义source  连接mongodb
        val dataStream = env.addSource(RatingMongoSource("recommender", "ratings"))

        // 4. 创建表环境
        val tableEnv = StreamTableEnvironment.create(env)
        // 6.1 历史热门
        historyHotProducts(tableEnv, dataStream)
        // 6.2 近期热门
        recentHotProducts(tableEnv, dataStream)

        // 7、执行任务
        env.execute("statistics recommender")
    }

    // 历史热门
    def historyHotProducts(tableEnv: StreamTableEnvironment, ratingStream: DataStream[Rating]): Unit = {
        val tableRating = tableEnv.fromDataStream(ratingStream, $"userId", $"productId")
        // 5. 创建视图，执行SQL
        tableEnv.createTemporaryView("ratings", tableRating)
        // 只保存前 100 热门数据
        val sql = "select productId, count(productId) as counts from ratings group by productId order by counts desc limit 100"
        // 执行
        val table = tableEnv.sqlQuery(sql)
        // 写表
        table.toRetractStream[Row].addSink(RateProductMongoSink("recommender", "rate_products")).setParallelism(1)
    }

    // 近期热门
    def recentHotProducts(tableEnv: StreamTableEnvironment, ratingStream: DataStream[Rating]): Unit = {
        val sourceStream = ratingStream.map(rating => {
            val format = new SimpleDateFormat("yyyyMM")
            val time = format.format(new Date(rating.timestamp * 1000L))
            (rating.userId, rating.productId, time.toInt)
        })

        val tableRating = tableEnv.fromDataStream(sourceStream, $"userId", $"productId", $"yearMonth")
        // 5. 创建视图，执行SQL
        tableEnv.createTemporaryView("recentRatings", tableRating)
        val sql = "select productId, count(productId) as counts, yearMonth from recentRatings group by yearMonth, productId order by yearMonth desc, counts desc"
        // 执行
        val table = tableEnv.sqlQuery(sql)
        // 写表
        table.toRetractStream[Row].addSink(RateRecentlyProductMongoSink("recommender", "rate_recently_products")).setParallelism(1)
    }
}
