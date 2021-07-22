package com.hadoop.study.flink.recommend.sources

import com.hadoop.study.flink.recommend.beans.Rating
import com.hadoop.study.flink.recommend.utils.ConnHelper
import com.mongodb.casbah.Imports.DBObject
import com.mongodb.casbah.MongoCollection
import org.apache.flink.api.common.io.statistics.BaseStatistics
import org.apache.flink.api.common.io.{DefaultInputSplitAssigner, RichInputFormat}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.io.{GenericInputSplit, InputSplit, InputSplitAssigner}


/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/22 15:55
 */

case class RatingMongoSource(db: String, collection: String) extends RichInputFormat[Rating, InputSplit] with ResultTypeQueryable[Rating] {

    private var mongoClient: MongoCollection = _

    private var hasNext = false

    private var index = 0

    private var resultSet: List[DBObject] = _

    override def getProducedType: TypeInformation[Rating] = TypeInformation.of(classOf[Rating])

    override def configure(parameters: Configuration): Unit = {}

    override def getStatistics(cachedStatistics: BaseStatistics): BaseStatistics = cachedStatistics

    override def createInputSplits(minNumSplits: Int): Array[InputSplit] = Array(new GenericInputSplit(0, 1))

    override def getInputSplitAssigner(inputSplits: Array[InputSplit]): InputSplitAssigner = new DefaultInputSplitAssigner(inputSplits)

    override def open(split: InputSplit): Unit = {
        mongoClient = ConnHelper.mongoClient(db)(collection)
        resultSet = mongoClient.find().toList
        index = 0
        hasNext = resultSet.nonEmpty
    }

    override def reachedEnd(): Boolean = !hasNext

    override def nextRecord(reuse: Rating): Rating = {
        val value = resultSet(index)
        index += 1
        hasNext = resultSet.size > index
        Rating(Integer.parseInt(value.get("userId").toString),
            Integer.parseInt(value.get("productId").toString),
            java.lang.Double.parseDouble(value.get("score").toString),
            Integer.parseInt(value.get("timestamp").toString),
            java.lang.Long.parseLong(value.get("createTime").toString))
    }

    override def close(): Unit = {}
}
