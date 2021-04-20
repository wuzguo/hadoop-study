package com.hadoop.study.scala.chapter21

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/20 14:46
 */

object Greeter {
    def greet(name: String)(implicit prompt: PreferredPrompt) = {
        println("Welcome, " + name + ". The system is ready.")
        println(prompt.preference)
    }
}

object Greeter2 {
    def greet(name: String)(implicit prompt: PreferredPrompt,
                            drink: PreferredDrink) = {

        println("Welcome, " + name + ". The system is ready.")
        print("But while you work, ")
        println("why not enjoy a cup of " + drink.preference + "?")
        println(prompt.preference)
    }
}
