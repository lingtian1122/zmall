package com.zhouxl.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * zhoumall-0923
 * <p>
 * com.zhouxl.interceptor
 */
public class TypeInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //1. 获取事件体
        String body = new String(event.getBody());
        //2.获取事件头信息
        Map<String, String> headers = event.getHeaders();

        if (body.contains("start")){
            headers.put("start","topic_start");
        }else {
            headers.put("event","topic_event");
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        //1. 创建一个新的事件数组
        ArrayList<Event> list = new ArrayList<>();
        for (Event event : events) {
            //2. 处理相应的事件
            Event event1 = intercept(event);
            list.add(event1);
        }

        return list;
    }

    @Override
    public void close() {

    }

    public static class builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new TypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
