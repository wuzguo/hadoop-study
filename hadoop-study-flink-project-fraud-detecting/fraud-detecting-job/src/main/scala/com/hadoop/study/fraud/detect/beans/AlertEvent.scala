package com.hadoop.study.fraud.detect.beans

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 11:09
 */

case class AlertEvent[TE, TV](ruleId: Int, rule: Rule, key: String, triggerEvent: TE, triggerValue: TV)
