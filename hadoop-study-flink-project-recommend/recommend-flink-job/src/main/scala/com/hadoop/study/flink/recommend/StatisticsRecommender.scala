package com.hadoop.study.flink.recommend

import com.hadoop.study.flink.recommend.sinks.RateProductMongoSink
import com.hadoop.study.flink.recommend.sources.RatingMongoSource
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.FieldExpression
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}
import org.apache.flink.types.Row

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
        // 3、打印数据
        // dataStream.print()

        // 4. 创建表环境
        val tableEnv = StreamTableEnvironment.create(env)
        val tableRating = tableEnv.fromDataStream(dataStream, $"userId", $"productId")
        tableRating.printSchema()
        // 5. 创建视图，执行SQL
        tableEnv.createTemporaryView("ratings", tableRating)

        // 6.1 历史热门
        historyHotProducts(tableEnv)

        // 7、执行任务
        env.execute("statistics recommender")
    }

    // 历史热门
    def historyHotProducts(tableEnv: StreamTableEnvironment): Unit = {
        // 只保存前 100 热门数据
        val sql = "select productId, count(productId) as counts from ratings group by productId order by counts desc limit 100"
        // 执行
        val table = tableEnv.sqlQuery(sql)
        // 写表
        table.toRetractStream[Row].addSink(RateProductMongoSink("recommender", "rate_products"))
    }
}
