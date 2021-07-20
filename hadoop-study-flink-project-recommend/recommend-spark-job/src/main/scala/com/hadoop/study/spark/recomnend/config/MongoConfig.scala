package com.hadoop.study.spark.recomnend.config

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/20 14:35
 */

/**
 * MongoDB连接配置
 *
 * @param uri MongoDB的连接uri
 * @param db  要操作的db
 */
case class MongoConfig(uri: String, db: String)
