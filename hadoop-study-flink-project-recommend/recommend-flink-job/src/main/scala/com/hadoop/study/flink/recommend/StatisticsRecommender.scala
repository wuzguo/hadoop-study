package com.hadoop.study.flink.recommend

import com.hadoop.study.flink.recommend.sources.RatingMongoSource
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

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
        dataStream.print()
        // 4、执行任务
        env.execute("statistics recommender")
    }
}
