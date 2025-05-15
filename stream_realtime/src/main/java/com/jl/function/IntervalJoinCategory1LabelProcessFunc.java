package com.jl.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

public class IntervalJoinCategory1LabelProcessFunc extends ProcessJoinFunction<JSONObject,JSONObject,JSONObject>{

    @Override
    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        JSONObject result = new JSONObject();

        if (left.getString("category1_id").equals(right.getString("id"))) {
            result.putAll(left);
            result.put("category1_name", right.getString("category1_name"));
        }
        out.collect(result);
    }
}
