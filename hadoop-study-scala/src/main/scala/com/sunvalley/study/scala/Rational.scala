package com.sunvalley.study.scala

class Rational(n: Int, d: Int) {
    require(d != 0)

    private val g = gcd(n.abs, d.abs)

    val number: Int = n / g
    val demon: Int = d / g

    println("created " + number + "/" + demon)

    override def toString = number + "/" + demon

    def this(n: Int) = this(n, 1)

    def add(that: Rational): Rational = {
        new Rational(number * that.demon + that.number * demon, demon * that.demon)
    }

    def +(that: Rational): Rational = {
        new Rational(number * that.demon + that.number * demon, demon * that.demon)
    }

    def * (that: Rational): Rational = new Rational(number * that.number, demon * that.demon)

    def lessThan(that: Rational) = this.number * that.demon < that.number * this.demon

    def max(that: Rational) = if (this.lessThan(that)) that else this

    private def gcd(a: Int, b: Int): Int = if (b == 0) a else gcd(b, a % b)

}
