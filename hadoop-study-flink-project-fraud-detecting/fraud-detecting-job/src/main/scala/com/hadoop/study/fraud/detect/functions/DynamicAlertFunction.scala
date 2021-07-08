package com.hadoop.study.fraud.detect.functions

import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 13:48
 */

 class DynamicAlertFunction extends KeyedBroadcastProcessFunction[String, Keyed[Tra]]
