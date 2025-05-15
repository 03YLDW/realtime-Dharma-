package com.jl.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.math.BigDecimal;
import java.math.RoundingMode;


public class CalculateWeightFunction extends RichMapFunction<JSONObject,JSONObject> {




    @Override
    public JSONObject map(JSONObject json) throws Exception {



        double price = json.getDoubleValue("split_total_amount");

        if (price<=1000 && price>=0){
            json.put("price_18~24", round(0.8 * 0.15));
            json.put("price_25~29", round(0.6 * 0.15));
            json.put("price_30~34", round(0.4 * 0.15));
            json.put("price_35~39", round(0.3 * 0.15));
            json.put("price_40~49", round(0.2 * 0.15));
            json.put("price_50", round(0.1 * 0.15));
        }else if(price<=4000 && price>1000){
            json.put("price_18~24", round(0.2 * 0.15));
            json.put("price_25~29", round(0.4 * 0.15));
            json.put("price_30~34", round(0.6 * 0.15));
            json.put("price_35~39", round(0.7 * 0.15));
            json.put("price_40~49", round(0.8 * 0.15));
            json.put("price_50", round(0.7 * 0.15));
        }else{
            json.put("price_18~24", round(0.1 * 0.15));
            json.put("price_25~29", round(0.2 * 0.15));
            json.put("price_30~34", round(0.3 * 0.15));
            json.put("price_35~39", round(0.4 * 0.15));
            json.put("price_40~49", round(0.5 * 0.15));
            json.put("price_50", round(0.6 * 0.15));
        }

        String btname = json.getString("btname");
        if (btname==null){
            btname="家庭与育儿";
        }
        switch (btname) {
            case "时尚与潮流":
                json.put("category_18~24", round(0.9 * 0.3));
                json.put("category_25~29", round(0.7 * 0.3));
                json.put("category_30~34", round(0.5 * 0.3));
                json.put("category_35~39", round(0.3 * 0.3));
                json.put("category_40~49", round(0.2 * 0.3));
                json.put("category_50", round(0.1    * 0.3));
                break;
            case "性价比":
                json.put("category_18~24", round(0.2 * 0.3));
                json.put("category_25~29", round(0.4 * 0.3));
                json.put("category_30~34", round(0.6 * 0.3));
                json.put("category_35~39", round(0.7 * 0.3));
                json.put("category_40~49", round(0.8 * 0.3));
                json.put("category_50", round(0.8    * 0.3));
                break;
            case "家庭与育儿":
                json.put("category_18~24", round(0.1 * 0.3));
                json.put("category_25~29", round(0.2 * 0.3));
                json.put("category_30~34", round(0.4 * 0.3));
                json.put("category_35~39", round(0.6 * 0.3));
                json.put("category_40~49", round(0.8 * 0.3));
                json.put("category_50", round(0.7    * 0.3));
                break;
            case "科技与数码":
                json.put("category_18~24", round(0.8 * 0.3));
                json.put("category_25~29", round(0.6 * 0.3));
                json.put("category_30~34", round(0.4 * 0.3));
                json.put("category_35~39", round(0.3 * 0.3));
                json.put("category_40~49", round(0.2 * 0.3));
                json.put("category_50", round(0.1    * 0.3));
                break;
            case "学习与发展":
                json.put("category_18~24", round(0.4 * 0.3));
                json.put("category_25~29", round(0.5 * 0.3));
                json.put("category_30~34", round(0.6 * 0.3));
                json.put("category_35~39", round(0.7 * 0.3));
                json.put("category_40~49", round(0.8 * 0.3));
                json.put("category_50", round(0.7    * 0.3));
                break;
            default:
                json.put("category_18~24", 0);
                json.put("category_25~29", 0);
                json.put("category_30~34", 0);
                json.put("category_35~39", 0);
                json.put("category_40~49", 0);
                json.put("category_50", 0);
        }

        String tm_name = json.getString("tm_name");

        switch (tm_name) {
            case "苹果12233":
                json.put("category1_18~24", round(0.9 * 0.2));
                json.put("category1_25~29", round(0.7 * 0.2));
                json.put("category1_30~34", round(0.5 * 0.2));
                json.put("category1_35~39", round(0.3 * 0.2));
                json.put("category1_40~49", round(0.2 * 0.2));
                json.put("category1_50", round(0.1    * 0.2));
                break;
            case "香奈儿":
                json.put("category1_18~24", round(0.2 * 0.2));
                json.put("category1_25~29", round(0.4 * 0.2));
                json.put("category1_30~34", round(0.6 * 0.2));
                json.put("category1_35~39", round(0.7 * 0.2));
                json.put("category1_40~49", round(0.8 * 0.2));
                json.put("category1_50", round(0.8    * 0.2));
                break;
            case "联想":
                json.put("brand_18~24", round(0.1 * 0.2));
                json.put("brand_25~29", round(0.2 * 0.2));
                json.put("brand_30~34", round(0.4 * 0.2));
                json.put("brand_35~39", round(0.6 * 0.2));
                json.put("brand_40~49", round(0.8 * 0.2));
                json.put("brand_50", round(0.7    * 0.2));
                break;
            case "Redmi165165651":
                json.put("brand_18~24", round(0.8 * 0.2));
                json.put("brand_25~29", round(0.6 * 0.2));
                json.put("brand_30~34", round(0.4 * 0.2));
                json.put("brand_35~39", round(0.3 * 0.2));
                json.put("brand_40~49", round(0.2 * 0.2));
                json.put("brand_50", round(0.1    * 0.2));
                break;
            case "金沙河":
                json.put("brand_18~24", round(0.4 * 0.2));
                json.put("brand_25~29", round(0.5 * 0.2));
                json.put("brand_30~34", round(0.6 * 0.2));
                json.put("brand_35~39", round(0.7 * 0.2));
                json.put("brand_40~49", round(0.8 * 0.2));
                json.put("brand_50", round(   0.7 * 0.2));
                break;
            default:
                json.put("brand_18~24", 0);
                json.put("brand_25~29", 0);
                json.put("brand_30~34", 0);
                json.put("brand_35~39", 0);
                json.put("brand_40~49", 0);
                json.put("brand_50", 0);
        }


        
        
        

        return json;


    }


    private static double round(double value) {
        return BigDecimal.valueOf(value)
                .setScale(3, RoundingMode.HALF_UP)
                .doubleValue();
    }
}
