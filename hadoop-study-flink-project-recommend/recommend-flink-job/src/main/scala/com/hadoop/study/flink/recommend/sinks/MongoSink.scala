package com.hadoop.study.flink.recommend.sinks

import com.hadoop.study.flink.recommend.utils.ConnHelper
import com.mongodb.casbah.MongoCollection
import lombok.extern.slf4j.Slf4j
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/22 15:07
 */

@Slf4j
case class MongoSink[T](db: String, collection: String) extends OutputFormat[T] {

    private var mongoClient: MongoCollection = _

    override def configure(parameters: Configuration): Unit = {}

    override def open(taskNumber: Int, numTasks: Int): Unit = mongoClient = ConnHelper.mongoClient(db)(collection)

    override def writeRecord(record: T): Unit = {
        //        mongoClient.insert(MongoDBObject("userId" -> userId, "recs" -> streamRecs.map(x => MongoDBObject("productId" -> x._1, "score" -> x._2))))
    }

    override def close(): Unit = {

    }
}
