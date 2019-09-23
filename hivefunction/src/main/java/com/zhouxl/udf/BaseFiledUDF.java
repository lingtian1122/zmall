package com.zhouxl.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * zhoumall-0923
 * <p>
 * com.zhouxl.udf
 * <p>
 * 对传进来的数据进行切分和拼接成新的字段
 *
 * 对数据解析，一条日志解析成     一个字符串【一堆17公共字段+18事件json数组+19时间戳】
 */
public class BaseFiledUDF extends UDF {

    public String evaluate(String line, String jsonkeys) {

        //创建一个 StringBuilder
        StringBuilder sb = new StringBuilder();

        try {
            //0. 切分后获取时间
            String[] splits = line.split("\\|");
            //1. 获取采集的时间
            String ts = splits[0];
            //2. 获取公共字段
            String jsonstrs = splits[1];
            //3. 创建一个json对象,将公共字段转换成json对象进行分析
            JSONObject jsonObject = new JSONObject(jsonstrs);
            //获取对应的公共字段对象
            JSONObject cmJsonObj = jsonObject.getJSONObject("cm");

            //4. 获取对应的jsonkeys对象
            String[] jskeys = jsonkeys.split(",");

            //5. 遍历获取对应的key和value
            for (String jskey : jskeys) {
                String jsvalue = cmJsonObj.getString(jskey);
                if (jsvalue == null) {
                    sb.append("\t");
                } else {
                    sb.append(jsvalue).append("\t");
                }
            }
            //6. 获取最大的json数组
            String et = jsonObject.getString("et");
            sb.append(et).append("\t");
            //7. 添加时间戳
            sb.append(ts);

        } catch (JSONException e) {
            e.printStackTrace();
        }

        return sb.toString();
    }

}
