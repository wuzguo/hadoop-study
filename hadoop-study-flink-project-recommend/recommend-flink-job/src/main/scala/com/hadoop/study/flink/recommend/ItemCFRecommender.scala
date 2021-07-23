package com.hadoop.study.flink.recommend

import com.hadoop.study.flink.recommend.beans.{ProductCount, ProductRecs, Rating, Recommendation}
import com.hadoop.study.flink.recommend.functions.SimilarityFunction
import com.hadoop.study.flink.recommend.sinks.ProductRecsMongoSink
import com.hadoop.study.flink.recommend.sources.RatingMongoSource
import org.apache.flink.api.common.functions.{FlatJoinFunction, FlatMapFunction, MapFunction, ReduceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Expressions.call
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}
import org.apache.flink.table.api.{FieldExpression, WithOperations}
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/23 14:49
 */

object ItemCFRecommender {

    def main(args: Array[String]): Unit = {

        // 1、获取流式环境变量
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 2、添加自定义source  连接mongodb
        val dataStream = env.addSource(RatingMongoSource("recommender", "ratings"))
        // 3、创建表环境
        val tableEnv = StreamTableEnvironment.create(env)

        // 4、计算ITEMCF
        calSimilarity(tableEnv, dataStream)
        // 5、执行任务
        env.execute("item cf recommender")
    }

    def calSimilarity(tableEnv: StreamTableEnvironment, ratingStream: DataStream[Rating]): Unit = {
        val countStream = ratingStream.flatMap(new FlatMapFunction[Rating, ProductCount] {
            override def flatMap(value: Rating, out: Collector[ProductCount]): Unit = ProductCount(value.userId, value.productId, value.score, 1)
        })

        val keyedStream = countStream.keyBy(_.userId).sum("score")
        val joinedStream = keyedStream.join(keyedStream)
          .where(_.userId).equalTo(_.userId)
          .window(TumblingEventTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
          .apply(new FlatJoinFunction[ProductCount, ProductCount, (Int, Int, Int, Int, Int)] {
              override def join(first: ProductCount, second: ProductCount, out: Collector[(Int, Int, Int, Int, Int)]): Unit = out.collect((first.userId, first.productId, first.count, second.productId, second.count))
          })

        // 转换
        val tableJoined = tableEnv.fromDataStream(joinedStream, $"userId", $"productId1", $"count1", $"productId2", $"count2")
        // 创建视图，执行SQL
        tableEnv.createTemporaryView("item_cf_recommends", tableJoined)
        // SQL
        val sql = "select productId1, productId2 ,count(userId) as coCount, avg(count1) as count1, avg(count2) as count2 from item_cf_recommends group by productId1, productId2"
        // 执行
        val table = tableEnv.sqlQuery(sql)
        table.printSchema()
        // 注册函数
        tableEnv.createTemporaryFunction("simCal", classOf[SimilarityFunction])
        val simTable = table.map(call("simCal", $"productId1", $"productId2", $"coCount", $"count1", $"count2"))
          .as($"productId1", $"product2", $"sim")
          .filter($"productId1" !== $"product2")

        val recsStream = simTable.toRetractStream[Row].map(new MapFunction[(Boolean, Row), ProductRecs] {
            override def map(value: (Boolean, Row)): ProductRecs = {
                val recs = ListBuffer[Recommendation]()
                recs += Recommendation(value._2.getField(1).toString.toInt, value._2.getField(2).toString.toDouble)
                ProductRecs(value._2.getField(0).toString.toInt, recs)
            }
        }).keyBy(_.productId).reduce(new ReduceFunction[ProductRecs] {
            override def reduce(value1: ProductRecs, value2: ProductRecs): ProductRecs = {
                val recs: mutable.Buffer[Recommendation] = value1.recs.toBuffer
                value2.recs.foreach(rec => recs += rec)
                recs.sortBy(_.score)(Ordering.Double.reverse)
                ProductRecs(value1.productId, recs)
            }
        })

        // sink
        recsStream.addSink(ProductRecsMongoSink("recommender", "rate_products")).setParallelism(1)
    }
}
