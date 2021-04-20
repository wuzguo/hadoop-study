package com.hadoop.study.scala.chapter21

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/20 14:49
 */

object JoesPrefs {

    implicit val prompt = new PreferredPrompt("Yes, master> ")

    implicit val drink = new PreferredDrink("tea")
}
