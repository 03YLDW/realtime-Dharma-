package com.jl.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

public class AllIntervalJoinFunction extends ProcessJoinFunction<JSONObject,JSONObject,JSONObject> {
    @Override
    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        JSONObject result = new JSONObject();

        System.out.println("左" + left + "右" + right);

        if (left.getString("user_id").equals(right.getString("uid"))) {
            result.putAll(left);
            result.put("os", right.getString("os"));
            result.put("ch", right.getString("ch"));
            result.put("pv", right.getString("pv"));
            result.put("md", right.getString("md"));
            result.put("search_item", right.getString("search_item"));
            result.put("ba", right.getString("ba"));
            result.put("ts", right.getString("ts"));
            System.out.println("result"+result);
        }
        out.collect(result);


    }
}
