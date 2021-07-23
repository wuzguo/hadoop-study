package com.hadoop.study.flink.recommend.sources

import com.hadoop.study.flink.recommend.beans.Rating
import com.hadoop.study.flink.recommend.utils.ConnHelper
import com.mongodb.casbah.MongoCollection
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}


/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/22 15:55
 */

case class RatingMongoSource(db: String, collection: String) extends RichSourceFunction[Rating] {

    private var mongoCollection: MongoCollection = _

    override def run(ctx: SourceFunction.SourceContext[Rating]): Unit = {
        val resultSet = mongoCollection.find()
        if (resultSet.nonEmpty) {
            resultSet.foreach(value => ctx.collect(Rating(value.get("userId").toString.toInt,
                value.get("productId").toString.toInt,
                value.get("score").toString.toDouble,
                value.get("timestamp").toString.toInt,
                value.get("createTime").toString.toLong)))
        }
    }

    override def cancel(): Unit = mongoCollection = null

    override def open(parameters: Configuration): Unit = mongoCollection = ConnHelper.mongoClient(db)(collection)
}
