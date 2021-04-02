package com.sunvalley.study;

import java.math.BigInteger;
import org.junit.FixMethodOrder;
import org.junit.Test;

/**
 * <B>说明：</B><BR>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/2 9:51
 */

@FixMethodOrder
public class TestChapter1 {

    private BigInteger factorial2(BigInteger x) {
        return x.equals(BigInteger.ZERO) ? BigInteger.ONE : x.multiply(factorial2(x.subtract(BigInteger.ONE)));
    }

    @Test
    public void testFactorial2() {
        System.out.println(factorial2(BigInteger.valueOf(100)));
    }
}
