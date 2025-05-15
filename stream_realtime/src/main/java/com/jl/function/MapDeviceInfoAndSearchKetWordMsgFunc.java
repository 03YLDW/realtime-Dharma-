package com.jl.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;

//设备信息处理


public class MapDeviceInfoAndSearchKetWordMsgFunc extends RichMapFunction<JSONObject,JSONObject> {
    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        JSONObject result = new JSONObject();
        //判断 topic_db中 是否包含 common字段
        if (jsonObject.containsKey("common")){
            JSONObject common = jsonObject.getJSONObject("common");
            //获取 用户id： uid    时间戳 ts
            result.put("uid",common.getString("uid") != null ? common.getString("uid") : "-1");
            result.put("ts",jsonObject.getLongValue("ts"));
            JSONObject deviceInfo = new JSONObject();
//            将common中的物无关字段过滤
            common.remove("sid");
            common.remove("mid");
            common.remove("is_new");
//            System.out.println("common------------------------>"+common);
//            将过滤后的common json对象 添加到新的json对象中
            deviceInfo.putAll(common);
            result.put("deviceInfo",deviceInfo);
            if(jsonObject.containsKey("page") && !jsonObject.getJSONObject("page").isEmpty()){
                JSONObject pageInfo = jsonObject.getJSONObject("page");
                if (pageInfo.containsKey("item_type") && pageInfo.getString("item_type").equals("keyword")){
                    String item = pageInfo.getString("item");
                    result.put("search_item",item);
                }
            }
        }
        JSONObject deviceInfo = result.getJSONObject("deviceInfo");
        String os = deviceInfo.getString("os").split(" ")[0];
        deviceInfo.put("os",os);


        return result;
    }
}
