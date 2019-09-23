package com.zhouxl.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * zhoumall-0923
 * <p>
 * com.zhouxl.udtf
 *
 * 解析一个json数组，传进来一个参数，返回的是多个json对象
 */
public class EventJsonUDTF extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        //0. 定义字段的集合
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("event_name");
        fieldNames.add("event_json");

        //定义字段类型的集合
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaShortObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaShortObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        //1. 取出传进来的参数，并转换成json对象
        String jsonStr = args[0].toString();
        try {
            JSONArray jsonArray = new JSONArray(jsonStr);

            //根据初始化输出类型定义一个String 数组
            String[] strings = new String[2];

            //3.遍历每一个json数组
            for (int i = 0; i < jsonStr.length(); i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                strings[0] = jsonObject.getString("en");
                strings[1] = jsonObject.toString();

                //将定义的输出数组进行输出
                forward(strings);
//                System.out.println(strings[0]);
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws HiveException {

    }

}
