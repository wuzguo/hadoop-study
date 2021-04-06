package com.sunvalley.study;

import org.junit.FixMethodOrder;
import org.junit.Test;

/**
 * <B>说明：</B><BR>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/2 17:24
 */

@FixMethodOrder
public class TestChapter3 {

    @Test
    public void testArrays() {
        String[] greetStrings = new String[3];
        greetStrings[0] = "Hello";
        greetStrings[1] = ", ";
        greetStrings[2] = "world!\n";
        for (int i = 0; i <= 2; i++) {
            System.out.print(greetStrings[i]);
        }
    }
}
