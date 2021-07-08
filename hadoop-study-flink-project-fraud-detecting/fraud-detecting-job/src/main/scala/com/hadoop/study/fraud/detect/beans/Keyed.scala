package com.hadoop.study.fraud.detect.beans

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 11:11
 */

case class Keyed[IN, KEY, ID](wrapped: IN, key: KEY, id: ID)
