package com.zhouxl.interceptor;


import com.zhouxl.utils.ETLUtil;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;

/**
 * zhoumall-0923
 * <p>
 * com.zhouxl.interceptor
 * <p>
 * 实现数据完整性的保证
 */
public class ETLinterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //1. 取出传进来的事件
        String body = new String(event.getBody());
        boolean flag;
        //2.分别对事件进行过滤
        if (body.contains("start")) {
            //过滤启动日志
            flag = ETLUtil.valiStartData(body);
        } else {
            //过滤事件日志
            flag = ETLUtil.valiEvenData(body);
        }

        if (flag) {
            return event;
        } else {
            return null;
        }
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        //1. 创建一个list
        ArrayList<Event> interceptors = new ArrayList<>();
        //2. 遍历传进来的数组
        for (Event event : events) {
            //判断事件的类型是否符合要求
            Event event1 = intercept(event);
            //将符合要求的事件写入到新的数组内
            interceptors.add(event1);
        }
        return interceptors;
    }

    @Override
    public void close() {

    }

    //创建对应的构造方法
    public static class builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new ETLinterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
