package com.zhouxl.utils;

import org.apache.commons.lang.math.NumberUtils;

/**
 * zhoumall-0923
 * <p>
 * com.zhouxl.utils.ETLUtil
 *
 * 创建对应的方法，处理不同的事件
 */
public class ETLUtil {
    /**
     * 处理启动日志数据
     * @param body 消息体
     * @return  true 表示数据完整；false表示数据不完整
     */
    public static boolean valiStartData(String body) {
        //1. 判空
        if (body == null) {
            return false;
        } else {
            return body.startsWith("{") && body.endsWith("}");
        }
    }

    /**
     * 处理事件日志
     * @param body 消息体
     * @return true 表示数据完整；false表示数据不完整
     */
    public static boolean valiEvenData(String body) {

        //1. 判空
        if (body == null) {
            return false;

        } else {
            String[] fields = body.split("\\|");
            //2.数字切分后不是2段，舍去
            if (fields.length != 2) {
                return false;
            }
            //2.1 数字1段不是13位纯数字，false
            if (!(fields[0].length() == 13) || !NumberUtils.isDigits(fields[0])) {
                return false;
            }
            //2.2 数字2段不是json格式，false
            return fields[1].startsWith("{") && fields[1].endsWith("}");
        }
    }
}
