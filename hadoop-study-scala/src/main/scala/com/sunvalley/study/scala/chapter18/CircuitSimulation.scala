package com.sunvalley.study.scala.chapter18

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/8 16:14
 */

abstract class CircuitSimulation extends BasicCircuitSimulation {

    def halfAdder(a: Wire, b: Wire, s: Wire, c: Wire) = {
        val d, e = new Wire
        orGate(a, b, d)
        andGate(a, b, c)
        inverter(c, e)
        andGate(d, e, s)
    }

    def fullAdder(a: Wire, b: Wire, cin: Wire, sum: Wire, cout: Wire) = {
        val s, c1, c2 = new Wire
        halfAdder(a, cin, s, c1)
        halfAdder(b, s, sum, c2)
        orGate(c1, c2, cout)
    }
}