package com.jl.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

public class IntervalJoinTrademarkLabelProcessFunc extends ProcessJoinFunction<JSONObject,JSONObject,JSONObject> {
    @Override
    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        JSONObject result = new JSONObject();

        if (left.getString("tm_id").equals(right.getString("id"))){
            result.putAll(left);
            result.put("tm_name",right.getString("tm_name"));
        }
        out.collect(result);
    }
}
