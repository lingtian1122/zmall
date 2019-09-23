package com.zhouxl.test;

import com.zhouxl.udf.BaseFiledUDF;

/**
 * zhoumall-0923
 * <p>
 * com.zhouxl.test
 */
public class BaseFieldTest {
    public static void main(String[] args) {
        String s = "123456|{\"cm\":{\"mid\":\"m1234\",\"uid\":\"u2345\"},\"et\":{\"uu\":\"uu123\",\"up\":\"234\"}}";
        BaseFiledUDF baseFiledUDF = new BaseFiledUDF();
        String s1 = baseFiledUDF.evaluate(s, "mid,uid");
        System.out.println(s1);

    }
}
