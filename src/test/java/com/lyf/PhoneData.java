package com.lyf;

import org.junit.Test;

import java.util.UUID;

/**
 * @author lyf
 * @date 2019/3/17 0017 下午 6:46
 */
public class PhoneData {

    public String r(int len) {
        return (Math.random()+"").substring(2,len+2);
    }

    public int r(int start, int end) {
        return (int) (Math.random() * (end - start) + start);
    }

    @Test
    public void create() {
        for (int i = 0; i < 50; i++) {
            String msg = r(13) + "\t13" + r(9) + "\t" + UUID.randomUUID() + "\t18\t15\t" + r(500, 800) + "\t" + r(0, 300);
            System.out.println(msg);
        }
    }

    @Test
    public void r() {
        System.out.println((Math.random()+"").substring(2,9+2));
    }
}
