package com.hadoop.study.flink.recommend.sinks

import com.hadoop.study.flink.recommend.beans.ProductRecs
import com.hadoop.study.flink.recommend.utils.ConnHelper
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import scala.collection.mutable.ListBuffer

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/23 15:53
 */

case class ProductRecsMongoSink(db: String, collection: String) extends RichSinkFunction[ProductRecs] {

    private var mongoCollection: MongoCollection = _

    override def open(parameters: Configuration): Unit = mongoCollection = ConnHelper.mongoClient(db)(collection)

    override def close(): Unit = mongoCollection = null

    override def invoke(value: ProductRecs, context: SinkFunction.Context): Unit = {
        // 删除数据
        mongoCollection.remove(MongoDBObject("productId" -> value.productId))

        // 构造集合
        val recs = ListBuffer[MongoDBObject]()
        value.recs.foreach(recommend => recs += MongoDBObject("productId" -> recommend.productId, "score" -> recommend.score))
        // 保存数据
        mongoCollection.insert(MongoDBObject("productId" -> value.productId, "recs" -> recs))
    }
}