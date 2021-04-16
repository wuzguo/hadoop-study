package com.sunvalley.study.scala.chapter19

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/8 16:39
 */

class Person(val firstName: String, val lastName: String) extends Ordered[Person] {

    def compare(that: Person) = {
        val lastNameComparison =
            lastName.compareToIgnoreCase(that.lastName)
        if (lastNameComparison != 0)
            lastNameComparison
        else
            firstName.compareToIgnoreCase(that.firstName)
    }

    override def toString = firstName + " " + lastName
}
