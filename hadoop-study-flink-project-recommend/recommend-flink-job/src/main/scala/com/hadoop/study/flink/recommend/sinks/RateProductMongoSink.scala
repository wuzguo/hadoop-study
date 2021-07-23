package com.hadoop.study.flink.recommend.sinks

import com.hadoop.study.flink.recommend.utils.ConnHelper
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.types.Row

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/22 15:07
 */

case class RateProductMongoSink(db: String, collection: String) extends RichSinkFunction[(Boolean, Row)] {

    private var mongoCollection: MongoCollection = _

    override def open(parameters: Configuration): Unit = mongoCollection = ConnHelper.mongoClient(db)(collection)

    override def close(): Unit = mongoCollection = null

    override def invoke(value: (Boolean, Row), context: SinkFunction.Context): Unit = {
        println(s"row: ${value._2.getField(0)},  ${value._2.getField(1)}")
        // 删除数据
        mongoCollection.remove(MongoDBObject("productId" -> value._2.getField(0)))
        // 保存数据
        mongoCollection.insert(MongoDBObject("productId" -> value._2.getField(0), "count" -> value._2.getField(1)))
    }
}
