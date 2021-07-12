package com.hadoop.study.fraud.detect.sources

import com.hadoop.study.fraud.detect.config.Config
import com.hadoop.study.fraud.detect.enums.SourceType
import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/12 15:20
 */

abstract class AbstractSource {

    def getSourceType(config: Config): SourceType.Value

    def create(config: Config): SourceFunction[String]
}
