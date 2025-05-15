package com.jl.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

public class IntervalJoinSkuInfoLabelProcessFunc extends ProcessJoinFunction<JSONObject,JSONObject,JSONObject> {
    @Override
    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        JSONObject result = new JSONObject();
        if (left.getString("sku_id").equals(right.getString("id"))){
            result.putAll(left);
            result.put("category3_id",right.getString("category3_id"));
            result.put("tm_id",right.getString("tm_id"));
        }
        out.collect(result);
    }
}
