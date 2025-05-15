package com.jl.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

public class IntervalJoinOrderInfoLabelProcessFunc extends ProcessJoinFunction<JSONObject,JSONObject,JSONObject> {
    @Override
    public void processElement(JSONObject jsonObject1, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        JSONObject result = new JSONObject();
        if (jsonObject1.getString("id").equals(jsonObject2.getString("order_id"))){
            result.putAll(jsonObject1);
            result.put("sku_id",jsonObject2.getString("sku_id"));
            result.put("split_total_amount",jsonObject2.getString("split_total_amount"));
        }
        out.collect(result);
    }
}
