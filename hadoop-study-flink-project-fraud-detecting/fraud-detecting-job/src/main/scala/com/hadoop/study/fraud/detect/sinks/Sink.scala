package com.hadoop.study.fraud.detect.sinks

import com.hadoop.study.fraud.detect.config.Config
import org.apache.flink.streaming.api.functions.sink.SinkFunction

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/12 9:31
 */

abstract class Sink {

    def create(config: Config): SinkFunction[String]
}
