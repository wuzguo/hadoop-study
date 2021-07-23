package com.hadoop.study.flink.recommend.utils

import com.mongodb.casbah.{MongoClient, MongoClientURI}
import redis.clients.jedis.{Jedis, Protocol}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/21 10:33
 */

object ConnHelper {

    // 懒变量定义，使用的时候才初始化
    lazy val jedis = new Jedis("localhost", Protocol.DEFAULT_PORT)

    lazy val mongoClient: MongoClient = MongoClient(MongoClientURI("mongodb://localhost:27017/recommender"))
}
