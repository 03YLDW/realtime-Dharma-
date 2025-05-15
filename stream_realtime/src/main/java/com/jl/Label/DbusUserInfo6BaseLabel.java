package com.jl.Label;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jl.bean.DimBaseCategory;
import com.jl.function.*;
import com.jl.utils.JdbcUtils;
import com.jl.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Connection;
import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;

import static java.time.format.DateTimeFormatter.ISO_DATE;

public class DbusUserInfo6BaseLabel {


    private static final List<DimBaseCategory> dim_base_categories;
    private static final Connection connection;

    private static final double device_rate_weight_coefficient = 0.1; // 设备权重系数
    private static final double search_rate_weight_coefficient = 0.15; // 搜索权重系数
    private static final double time_rate_weight_coefficient = 0.1;    // 时间权重系数
    private static final double amount_rate_weight_coefficient = 0.15;    // 价格权重系数
    private static final double brand_rate_weight_coefficient = 0.2;    // 品牌权重系数
    private static final double category_rate_weight_coefficient = 0.3; // 类目权重系数




    static {
        try {
            connection = JdbcUtils.getMySQLConnection(
                    "jdbc:mysql://cdh03:3306",
                    "root",
                        "root");
            //将一级品类表 二级品类表  三级品类表 关联
            String sql = "select b3.id,                          \n" +
                    "            b3.name as b3name,              \n" +
                    "            b2.name as b2name,              \n" +
                    "            b1.name as b1name               \n" +
                    "     from realtime_v1.base_category3 as b3  \n" +
                    "     join realtime_v1.base_category2 as b2  \n" +
                    "     on b3.category2_id = b2.id             \n" +
                    "     join realtime_v1.base_category1 as b1  \n" +
                    "     on b2.category1_id = b1.id";
            //封装到实体类中
            dim_base_categories = JdbcUtils.queryList2(connection, sql, DimBaseCategory.class, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
//topic_db数据



        SingleOutputStreamOperator<String> kfk_cdc_source = env.fromSource(
                        //1. 创建 KafkaSource
                        KafkaUtils.buildKafkaSecureSource(
                                "cdh01:9092",
                                "topic_db",
                                new Date().toString(),
                                OffsetsInitializer.earliest()
                        ),
                        //2. 设置 WatermarkStrategy
                        // WatermarkStrategy 的作用
                        //事件时间（Event Time）：使用数据自身携带的时间戳（如 ts_ms）作为时间基准，而非处理时间（Processing Time）。
                        //水位线（Watermark）：一种逻辑时钟机制，用于跟踪事件时间的进展，解决乱序数据带来的计算延迟问题。
                        //功能：告诉 Flink 如何从数据中提取事件时间戳，并生成水位线以推动时间窗口的触发。
                        //定义允许的最大乱序时间为 3 秒
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                //withTimestampAssigner
                                //从原始数据（event）中提取事件时间戳，赋值给 Flink 内部的时间属性。
                                // 代码逻辑：
                                //将原始数据（String 类型）解析为 JSONObject。
                                //检查是否存在 ts_ms 字段，并尝试将其转为 long 类型时间戳。
                                //若解析失败（如字段缺失或格式错误），打印错误日志并返回 0L。
                                .withTimestampAssigner((event, timestamp) -> {

                                            JSONObject jsonObject = JSONObject.parseObject(event);

                                            if (event != null && jsonObject.containsKey("tm_ms")) {
                                                try {
                                                    return jsonObject.getLong("tm_ms");
                                                } catch (Exception e) {
                                                    e.printStackTrace();
                                                    //将错误信息打印出来
                                                    System.out.println("Failed to parse event as JSON or get ts_ms: " + event);
                                                    return 0L;
                                                }
                                            } else {
                                                return 0L;
                                            }
                                        }
                                ),
                        //3. 设置 Source Name
                        "kfk_cdc_db_source"
                ).uid("kfk_cdc_db_source")
                .name("kfk_cdc_db_source");

        //  读取topic_db
        SingleOutputStreamOperator<String> sourceLog = env.fromSource(
                        KafkaUtils.buildKafkaSecureSource(
                                "cdh01:9092",
                                "topic_log",
                                new Date().toString(),
                                OffsetsInitializer.earliest()
                        ),
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((event, timestamp) -> {
                                            JSONObject jsonObject = JSONObject.parseObject(event);
                                            if (event != null && jsonObject.containsKey("tm_ms")) {
                                                try {
                                                    return jsonObject.getLong("tm_ms");
                                                } catch (Exception e) {
                                                    e.printStackTrace();
                                                    System.out.println("Failed to parse event as JSON or get ts_ms: " + event);
                                                    return 0L;
                                                }
                                            }
                                            return 0L;
                                        }
                                ),
                        "kfk_log_source"
                ).uid("sourceLog")
                .name("sourceLog");

//        sourceLog.print();



        SingleOutputStreamOperator<JSONObject> dataPageLogConvertJsonDs = sourceLog.map(JSON::parseObject)
                .uid("convert json page log")
                .name("convert json page log");



        //todo 设备信息 + 关键词搜索

        //设备信息处理
        SingleOutputStreamOperator<JSONObject> logDeviceInfoDs = dataPageLogConvertJsonDs.map(new MapDeviceInfoAndSearchKetWordMsgFunc())
                .uid("get device info & search")
                .name("get device info & search");

        //        logDeviceInfoDs.print();
        SingleOutputStreamOperator<JSONObject> filterNotNullUidLogPageMsg = logDeviceInfoDs.filter(data -> !data.getString("uid").isEmpty());


        KeyedStream<JSONObject, String> keyedStreamLogPageMsg = filterNotNullUidLogPageMsg.keyBy(data -> data.getString("uid"));

        SingleOutputStreamOperator<JSONObject> processStagePageLogDs = keyedStreamLogPageMsg.process(new ProcessFilterRepeatTsDataFunc());
//        processStagePageLogDs.print("wwwwwwwwwwwwwwwwwww");
//                                                                                         keyBy将数据流按照uid分区，保证相同uid的数据进入同一个处理实例。此时数据还是多个记录，只是分区了。
        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = processStagePageLogDs.keyBy(data -> data.getString("uid"))
//                这个函数内部维护了每个uid的pv状态和字段集合。每次处理元素时，会更新pv和字段集合，
//                然后输出一个包含当前状态的JSON对象。这里每次处理一条记录就会输出一次，所以输出可能还是多个相同uid的记录，但pv和字段集合会逐步累积。
                .process(new AggregateUserDataProcessFunction())
//                再次按uid分区，这一步可能不是必要的，因为数据已经被第一个keyBy分区过了，但可能因为后续的窗口操作需要重新分区。
                .keyBy(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
//                这里使用reduce函数，每次取最新的记录。由于窗口内的数据会被合并，最终每个窗口每个uid只会保留最后一条记录。
//                导致 uid 唯一的操作
//                关键操作：窗口（Window）与归约（Reduce）的组合
                .reduce((value1, value2) -> value2)
                .uid("win 2 minutes page count msg")
                .name("win 2 minutes page count msg");

//        win2MinutesPageLogsDs.print("wwwwwwwwwwwwwwwwwww");
//{"uid":"470","os":"Android","ch":"xiaomi","pv":4,"md":"vivo IQOO Z6x ","search_item":"心相印纸抽","ba":"vivo"}

        //todo 设备打分模型
        SingleOutputStreamOperator<JSONObject>  equipmentMap = win2MinutesPageLogsDs.map(new MapDeviceAndSearchMarkModelFunc(dim_base_categories, device_rate_weight_coefficient, search_rate_weight_coefficient));

//{"device_35_39":0.04,"os":"iOS","device_50":0.02,"search_25_29":0,"ch":"Appstore","pv":1,"device_30_34":0.05,"device_18_24":0.07,"search_50":0,"search_40_49":0,"uid":"249","device_25_29":0.06,"md":"iPhone 14 Plus","search_18_24":0,"judge_os":"iOS","search_35_39":0,"device_40_49":0.03,"search_item":"","ba":"iPhone","search_30_34":0}


        SingleOutputStreamOperator<JSONObject> kfk_source = kfk_cdc_source.map(JSONObject::parseObject)
                .uid("parseObject")
                .name("parseObject");
        // 过滤出 user_info
        SingleOutputStreamOperator<JSONObject> userinfoDs = kfk_source.filter(data -> data.getJSONObject("source").getString("table").equals("user_info"))
                .uid("userinfoDs")
                .name("userinfoDs");


        SingleOutputStreamOperator<JSONObject> orderinfoDs = kfk_source.filter(data -> data.getJSONObject("source").getString("table").equals("order_info"))
                .uid("orderinfoDs")
                .name("orderinfoDs");
//        orderinfoDs.print("orderinfoDs------>");
        SingleOutputStreamOperator<JSONObject> orderDetailDs = kfk_source.filter(data -> data.getJSONObject("source").getString("table").equals("order_detail"))
                .uid("orderdetailDs")
                .name("orderdetailDs");

        // TODO: 2025/5/14  orderInfo  订单表
        SingleOutputStreamOperator<JSONObject> orderInfoMap = orderinfoDs.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject value) throws Exception {
                JSONObject jsonObject = new JSONObject();
                if (value.containsKey("after") && value.getJSONObject("after") != null ) {
                    JSONObject after = value.getJSONObject("after");
                    jsonObject.put("id", after.getString("id"));
                    jsonObject.put("order_status", after.getString("order_status"));
                    jsonObject.put("payment_way", after.getString("payment_way"));
                    jsonObject.put("consignee", after.getString("consignee"));
                    jsonObject.put("total_amount", after.getDouble("total_amount"));
                    jsonObject.put("user_id", after.getString("user_id"));
                    jsonObject.put("trade_body", after.getString("trade_body"));
                    jsonObject.put("create_time", after.getString("create_time"));
                    jsonObject.put("province_id", after.getString("province_id"));
                    jsonObject.put("ts_ms", value.getString("ts_ms"));
                }

                return jsonObject;
            }
        }).filter(json -> !json.isEmpty())
                .uid("orderInfoMap")
                .name("orderInfoMap");

//        orderInfoMap.print("orderInfoMap========>");
        // TODO: 2025/5/14  orderDetail  订单明细
        SingleOutputStreamOperator<JSONObject> orderDetailMap = orderDetailDs.map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        JSONObject jsonObject = new JSONObject();
                        if (value.containsKey("after") && value.getJSONObject("after") != null ) {
                            JSONObject after = value.getJSONObject("after");
                            jsonObject.put("order_id", after.getString("order_id"));
                            jsonObject.put("sku_id", after.getString("sku_id"));
                            jsonObject.put("split_total_amount", after.getDouble("split_total_amount"));
                        }
                        return jsonObject;
                    }
                }).filter(json -> !json.isEmpty())
                .uid("orderDetail")
                .name("orderDetail");

//        orderDetailMap.print("orderDetail========>");
//        {"sku_id":"2","order_id":"1814","ts_ms":"1747055322011","split_total_amount":6499.0}
        SingleOutputStreamOperator<JSONObject> finalOrderInfoDs = orderInfoMap.filter(data -> data.containsKey("id") && !data.getString("id").isEmpty());
        SingleOutputStreamOperator<JSONObject> finalOrderDetailSupDs = orderDetailMap.filter(data -> data.containsKey("order_id") && !data.getString("order_id").isEmpty());
        //分组
        KeyedStream<JSONObject, String> keyedStreamOrderInfoDs = finalOrderInfoDs.keyBy(data -> data.getString("id"));
        KeyedStream<JSONObject, String> keyedStreamOrderDetailDs = finalOrderDetailSupDs.keyBy(data -> data.getString("order_id"));
        //订单表和订单明细关联
        SingleOutputStreamOperator<JSONObject> processIntervalJoinOrderInfo6BaseMessageDs = keyedStreamOrderInfoDs.intervalJoin(keyedStreamOrderDetailDs)
                .between(Time.minutes(-5), Time.minutes(5))
                .process(new IntervalJoinOrderInfoLabelProcessFunc());

//        processIntervalJoinOrderInfo6BaseMessageDs.print("processIntervalJoinOrderInfo6BaseMessageDs");
//{"order_status":"1004","payment_way":"3501","consignee":"秦艺咏","create_time":"1744133348000","total_amount":17167.0,"user_id":"109","province_id":"1","trade_body":"小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+512GB 冷杉绿 5G手机等3件商品","sku_id":"29","id":"2028","ts_ms":"1747055322307","split_total_amount":"69.0"}


        // TODO: 2025/5/15 关联sku表


        SingleOutputStreamOperator<JSONObject> skuInfoDs = kfk_source.filter(data -> data.getJSONObject("source").getString("table").equals("sku_info"))
                .uid("skuInfoDs")
                .name("skuInfoDs");

        SingleOutputStreamOperator<JSONObject> skuInfoMap = skuInfoDs.map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        JSONObject jsonObject = new JSONObject();
                        if (value.containsKey("after") && value.getJSONObject("after") != null ) {
                            JSONObject after = value.getJSONObject("after");
                            jsonObject.put("id", after.getString("id"));
                            jsonObject.put("category3_id", after.getString("category3_id"));
                            jsonObject.put("tm_id", after.getString("tm_id"));
                        }

                        return jsonObject;
                    }
                }).filter(json -> !json.isEmpty())
                .uid("skuInfoMap")
                .name("skuInfoMap");

//                skuInfoMap.print("skuInfoMap");



                SingleOutputStreamOperator<JSONObject> finalIntervalJoinOrderInfoDs = processIntervalJoinOrderInfo6BaseMessageDs.filter(data -> data.containsKey("sku_id") && !data.getString("sku_id").isEmpty());
                SingleOutputStreamOperator<JSONObject> finalSkuInfoDs = skuInfoMap.filter(data -> data.containsKey("id") && !data.getString("id").isEmpty());
                //分组
                KeyedStream<JSONObject, String> keyedStreamIntervalJoinOrderInfoDs = finalIntervalJoinOrderInfoDs.keyBy(data -> data.getString("sku_id"));
                KeyedStream<JSONObject, String> keyedStreamSkuInfoDs = finalSkuInfoDs.keyBy(data -> data.getString("id"));
                //订单表和订单明细关联
                SingleOutputStreamOperator<JSONObject> processIntervalJoinSkuInfoDs = keyedStreamIntervalJoinOrderInfoDs.intervalJoin(keyedStreamSkuInfoDs)
                        .between(Time.minutes(-5), Time.minutes(5))
                        .process(new IntervalJoinSkuInfoLabelProcessFunc());

//                 processIntervalJoinSkuInfoDs.print("processIntervalJoinSkuInfoDs");

        // TODO: 2025/5/15  关联品牌表


        SingleOutputStreamOperator<JSONObject> trademarkDs = kfk_source.filter(data -> data.getJSONObject("source").getString("table").equals("base_trademark"))
                .uid("trademarkDs")
                .name("trademarkDs");

        SingleOutputStreamOperator<JSONObject> trademarkMap = trademarkDs.map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        JSONObject jsonObject = new JSONObject();
                        if (value.containsKey("after") && value.getJSONObject("after") != null ) {
                            JSONObject after = value.getJSONObject("after");
                            jsonObject.put("id", after.getString("id"));
                            jsonObject.put("tm_name", after.getString("tm_name"));
                        }

                        return jsonObject;
                    }
                }).filter(json -> !json.isEmpty())
                .uid("trademarkMap")
                .name("trademarkMap");

//                skuInfoMap.print("skuInfoMap");



        SingleOutputStreamOperator<JSONObject> finalIntervalJoinSkuInfoDs = processIntervalJoinSkuInfoDs.filter(data -> data.containsKey("tm_id") && !data.getString("tm_id").isEmpty());
        SingleOutputStreamOperator<JSONObject> finalTrademarkDs = trademarkMap.filter(data -> data.containsKey("id") && !data.getString("id").isEmpty());
        //分组
        KeyedStream<JSONObject, String> keyedStreamIntervalJoinSkuInfoDs = finalIntervalJoinSkuInfoDs.keyBy(data -> data.getString("tm_id"));
        KeyedStream<JSONObject, String> keyedStreamTrademarkDs = finalTrademarkDs.keyBy(data -> data.getString("id"));
        //订单表和订单明细关联
        SingleOutputStreamOperator<JSONObject> processIntervalJoinTrademarkDs = keyedStreamIntervalJoinSkuInfoDs.intervalJoin(keyedStreamTrademarkDs)
                .between(Time.minutes(-5), Time.days(5))
                .process(new IntervalJoinTrademarkLabelProcessFunc());

//        processIntervalJoinTrademarkDs.print("processIntervalJoinTrademarkDs");
//{"payment_way":"3501","consignee":"秦艺咏","create_time":"1744133348000","sku_id":"16","tm_name":"联想","order_status":"1004","tm_id":"3","total_amount":17167.0,"user_id":"109","province_id":"1","trade_body":"小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+512GB 冷杉绿 5G手机等3件商品","id":"2028","category3_id":"287","ts_ms":"1747055322307","split_total_amount":"10599.0"}


        // TODO: 2025/5/15  关联 三级分类表


        SingleOutputStreamOperator<JSONObject> category3Ds = kfk_source.filter(data -> data.getJSONObject("source").getString("table").equals("base_category3"))
                .uid("category3Ds")
                .name("category3Ds");

        SingleOutputStreamOperator<JSONObject> category3Map = category3Ds.map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        JSONObject jsonObject = new JSONObject();
                        if (value.containsKey("after") && value.getJSONObject("after") != null ) {
                            JSONObject after = value.getJSONObject("after");
                            jsonObject.put("id", after.getString("id"));
                            jsonObject.put("category2_id", after.getString("category2_id"));
                        }

                        return jsonObject;
                    }
                }).filter(json -> !json.isEmpty())
                .uid("category3Map")
                .name("category3Map");

//                skuInfoMap.print("skuInfoMap");



        SingleOutputStreamOperator<JSONObject> finalIntervalJoinTrademarkDs = processIntervalJoinTrademarkDs.filter(data -> data.containsKey("category3_id") && !data.getString("category3_id").isEmpty());
        SingleOutputStreamOperator<JSONObject> finalCategory3Ds = category3Map.filter(data -> data.containsKey("id") && !data.getString("id").isEmpty());
        //分组
        KeyedStream<JSONObject, String> keyedStreamIntervalJoinTrademarkDs = finalIntervalJoinTrademarkDs.keyBy(data -> data.getString("category3_id"));
        KeyedStream<JSONObject, String> keyedStreamCategory3Ds = finalCategory3Ds.keyBy(data -> data.getString("id"));
        //订单表和订单明细关联
        SingleOutputStreamOperator<JSONObject> processIntervalJoinCategory3Ds= keyedStreamIntervalJoinTrademarkDs.intervalJoin(keyedStreamCategory3Ds)
                .between(Time.minutes(-5), Time.days(5))
                .process(new IntervalJoinCategory3LabelProcessFunc());

//        processIntervalJoinCategory3Ds.print("processIntervalJoinCategory3Ds");
//{"payment_way":"3501","consignee":"于博诚","create_time":"1744133346000","sku_id":"14","tm_name":"联想","order_status":"1002","tm_id":"3","total_amount":11749.0,"user_id":"542","province_id":"14","name":"游戏本","trade_body":"联想（Lenovo） 拯救者Y9000P 2022 16英寸游戏笔记本电脑 i9-12900H RTX3070Ti 钛晶灰等1件商品","id":"2001","category3_id":"287","ts_ms":"1747055322186","split_total_amount":"11749.0"}


        // TODO: 2025/5/15 关联二级分类表



        SingleOutputStreamOperator<JSONObject> category2Ds = kfk_source.filter(data -> data.getJSONObject("source").getString("table").equals("base_category2"))
                .uid("category2Ds")
                .name("category2Ds");

        SingleOutputStreamOperator<JSONObject> category2Map = category2Ds.map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        JSONObject jsonObject = new JSONObject();
                        if (value.containsKey("after") && value.getJSONObject("after") != null ) {
                            JSONObject after = value.getJSONObject("after");
                            jsonObject.put("id", after.getString("id"));
                            jsonObject.put("category1_id", after.getString("category1_id"));
                        }

                        return jsonObject;
                    }
                }).filter(json -> !json.isEmpty())
                .uid("category2Map")
                .name("category2Map");

//                skuInfoMap.print("skuInfoMap");



        SingleOutputStreamOperator<JSONObject> finalIntervalJoinCategory3Ds = processIntervalJoinCategory3Ds.filter(data -> data.containsKey("category2_id") && !data.getString("category2_id").isEmpty());
        SingleOutputStreamOperator<JSONObject> finalCategory2Ds = category2Map.filter(data -> data.containsKey("id") && !data.getString("id").isEmpty());
        //分组
        KeyedStream<JSONObject, String> keyedStreamIntervalJoinCategory3DDs = finalIntervalJoinCategory3Ds.keyBy(data -> data.getString("category2_id"));
        KeyedStream<JSONObject, String> keyedStreamCategory2Ds = finalCategory2Ds.keyBy(data -> data.getString("id"));
        //订单表和订单明细关联
        SingleOutputStreamOperator<JSONObject> processIntervalJoinCategory2Ds= keyedStreamIntervalJoinCategory3DDs.intervalJoin(keyedStreamCategory2Ds)
                .between(Time.minutes(-5), Time.days(5))
                .process(new IntervalJoinCategory2LabelProcessFunc());

//        processIntervalJoinCategory2Ds.print("processIntervalJoinCategory2Ds");





        // TODO: 2025/5/15 关联一级分类表


        SingleOutputStreamOperator<JSONObject> category1Ds = kfk_source.filter(data -> data.getJSONObject("source").getString("table").equals("base_category1"))
                .uid("category1Ds")
                .name("category1Ds");

        SingleOutputStreamOperator<JSONObject> category1Map = category1Ds.map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        JSONObject jsonObject = new JSONObject();
                        if (value.containsKey("after") && value.getJSONObject("after") != null ) {
                            JSONObject after = value.getJSONObject("after");
                            jsonObject.put("id", after.getString("id"));
                            jsonObject.put("category1_name", after.getString("name"));
                        }

                        return jsonObject;
                    }
                }).filter(json -> !json.isEmpty())
                .uid("category1Map")
                .name("category1Map");

//                skuInfoMap.print("skuInfoMap");



        SingleOutputStreamOperator<JSONObject> finalIntervalJoinCategory2Ds = processIntervalJoinCategory2Ds.filter(data -> data.containsKey("category1_id") && !data.getString("category1_id").isEmpty());
        SingleOutputStreamOperator<JSONObject> finalCategory1Ds = category1Map.filter(data -> data.containsKey("id") && !data.getString("id").isEmpty());
        //分组
        KeyedStream<JSONObject, String> keyedStreamIntervalJoinCategory2Ds = finalIntervalJoinCategory2Ds.keyBy(data -> data.getString("category1_id"));
        KeyedStream<JSONObject, String> keyedStreamCategory1Ds = finalCategory1Ds.keyBy(data -> data.getString("id"));
        //关联一级品牌表
        SingleOutputStreamOperator<JSONObject> processIntervalJoinCategory1Ds= keyedStreamIntervalJoinCategory2Ds.intervalJoin(keyedStreamCategory1Ds)
                .between(Time.minutes(-5), Time.days(5))
                .process(new IntervalJoinCategory1LabelProcessFunc());

//        processIntervalJoinCategory1Ds.print("processIntervalJoinCategory1Ds");

        // TODO: 2025/5/15  类目，品牌，价格，时间权重计算
        SingleOutputStreamOperator<JSONObject> processIntervalJoinWeight = processIntervalJoinCategory1Ds.map(new CalculateWeightFunction());




        win2MinutesPageLogsDs.print("%55555555555555555555555");
//{"payment_way":"3501","consignee":"于盛雄","create_time":"1744126134000","sku_id":"10","tm_name":"苹果12233","category1_id":"2","order_status":"1002","tm_id":"2","total_amount":8197.0,"user_id":"27","province_id":"16","category1_name":"手机","trade_body":"Apple iPhone 12 (A2404) 64GB 蓝色 支持移动联通电信5G 双卡双待手机等1件商品","id":"1697","category3_id":"61","ts_ms":"1747055321962","category2_id":"13","split_total_amount":"8197.0"}


        SingleOutputStreamOperator<JSONObject> finalIntervalJoinCategory1Ds = processIntervalJoinCategory1Ds.filter(data -> data.containsKey("user_id") && !data.getString("user_id").isEmpty());
        SingleOutputStreamOperator<JSONObject> finalMinutesPageLogsDs = win2MinutesPageLogsDs.filter(data -> data.containsKey("uid") && !data.getString("uid").isEmpty());
//        finalIntervalJoinCategory1Ds.print("000000000000000000000");
//        finalMinutesPageLogsDs.print("999999999999999999999");

        //分组
//        KeyedStream<JSONObject, String> keyedStreamIntervalJoinCategory1Ds = finalIntervalJoinCategory1Ds.keyBy(data -> data.getString("user_id"));
//        KeyedStream<JSONObject, String> keyedStreamPageLogsDs = finalMinutesPageLogsDs.keyBy(data -> data.getString("uid"));
//        keyedStreamIntervalJoinCategory1Ds.print("!1111111111111");
//        keyedStreamPageLogsDs.print("2222222222222222222222");
//
//        SingleOutputStreamOperator<JSONObject> AllIntervalJoin= keyedStreamPageLogsDs.intervalJoin(keyedStreamIntervalJoinCategory1Ds)
//                .between(Time.days(-2), Time.days(2))
//                .process(new AllIntervalJoinFunction());
//
//        AllIntervalJoin.print("all");

//        userinfoDs.print();
//        {"op":"c","after":{"birthday":12516,"gender":"M","create_time":1744133347000,"login_name":"8o958y5","nick_name":"富顺","name":"乐富顺","user_level":"1","phone_num":"13736681181","id":584,"email":"8o958y5@263.net"},"source":{"thread":934,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000053","connector":"mysql","pos":3365754,"name":"mysql_binlog_source","row":0,"ts_ms":1746437347000,"snapshot":"false","db":"realtime_v1","table":"user_info"},"ts_ms":1747055322190}
//        "birthday":12516  将生日转换为  年 月 日
        SingleOutputStreamOperator<JSONObject> userinfoBD = userinfoDs.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject value){
                JSONObject after = value.getJSONObject("after");
                // 检查after字典是否非空 且包含"birthday"键
                if (after!=null && after.containsKey("birthday")){
                    Integer birthday1 = after.getInteger("birthday");
                    if (birthday1!=null){
                        LocalDate localDate = LocalDate.ofEpochDay(birthday1);
                        after.put("birthday",localDate.format(DateTimeFormatter.ISO_DATE));
                    }
                }
                return  value;
            }
        });

//        userinfoBD.print();
//        {"op":"c","after":{   "birthday":"1971-01-08"   ,"gender":"M","create_time":1744125833000,"login_name":"kzffh62c","nick_name":"博诚","name":"金博诚","user_level":"3","phone_num":"13438591571","id":423,"email":"kzffh62c@googlemail.com"},"source":{"thread":148,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000050","connector":"mysql","pos":3403508,"name":"mysql_binlog_source","row":0,"ts_ms":1746343433000,"snapshot":"false","db":"realtime_v1","table":"user_info"},"ts_ms":1747055321709}

        // 过滤出 user_info_sup_msg表
        SingleOutputStreamOperator<JSONObject> user_info_sup_msg = kfk_source.filter(data -> data.getJSONObject("source").getString("table").equals("user_info_sup_msg"));
//        user_info_sup_msg.print();

        //首先将user_info中需要的字段进行封装，包括用户id，用户名，用户等级，用户昵称，手机号，邮箱，性别，生日，年代，星座
        //其中 年龄，星座，年代是另外计算出来的
        SingleOutputStreamOperator<JSONObject> newUserInfoDs = userinfoBD.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject value)  {
                JSONObject newJSON = new JSONObject();

                if (value.containsKey("after") && value.getJSONObject("after") != null) {
                    JSONObject after = value.getJSONObject("after");
                    newJSON.put("uid", after.getString("id"));
                    newJSON.put("name", after.getString("name"));
                    newJSON.put("user_level", after.getString("user_level"));
                    newJSON.put("login_name", after.getString("login_name"));
                    newJSON.put("phone_num", after.getString("phone_num"));
                    newJSON.put("email", after.getString("email"));
                    newJSON.put("gender", after.getString("gender") != null ? after.getString("gender") : "home");
                    newJSON.put("birthday", after.getString("birthday"));
                    newJSON.put("ts_ms", value.getJSONObject("source").getString("ts_ms"));
                    String birthday = after.getString("birthday");
                    if (birthday != null && !birthday.isEmpty()) {
//                         将字符串形式的生日转换为LocalDate对象
//                        ISO_DATE 是 Java 中 DateTimeFormatter 类的一个标准格式常量，表示 ISO 本地日期格式，其格式为 yyyy-MM-dd。
                        LocalDate birthday1 = LocalDate.parse(birthday, ISO_DATE);
                        // 获取当前日期，使用上海时区
                        LocalDate now = LocalDate.now(ZoneId.of("Asia/Shanghai"));
                        int age = calculateAge(birthday1, now);
                        //计算星座
                        String constellation = constellation(birthday1);
                        //计算年代
                        int decade = birthday1.getYear() / 10 * 10;
                        //年龄
                        newJSON.put("age", age);
                        //年代
                        newJSON.put("decade", decade);
                        //星座
                        newJSON.put("constellation", constellation);

                    }
                }
                return newJSON;
            }
        });
//        newUserInfoDs.print();
//{"birthday":"2007-06-08","decade":2000,"login_name":"nla0rjjhu1","gender":"M","constellation":"双子座","name":"令狐文","user_level":"1","phone_num":"13468892149","id":"377","email":"nla0rjjhu1@googlemail.com","ts_ms":"1745903788000","age":17}
        //TODO  user_info_sup_msg 获取身高，体重
        SingleOutputStreamOperator<JSONObject> userSupMsgDs = user_info_sup_msg.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject value) {
                JSONObject result = new JSONObject();
                if (value.containsKey("after") && value.getJSONObject("after") != null) {
                    JSONObject after = value.getJSONObject("after");
                    result.put("uid", after.getString("uid"));
                    result.put("unit_height", after.getString("unit_height"));
                    result.put("create_ts", after.getLong("create_ts"));
                    result.put("weight", after.getString("weight"));
                    result.put("unit_weight", after.getString("unit_weight"));
                    result.put("height", after.getString("height"));
                    result.put("ts_ms", value.getLong("ts_ms"));
                }
                return result;
            }
        }).uid("userSupMsgDs")
                .name("userSupMsgDs");

//        userSupMsgDs.print();
        // 过滤掉uid为空的数据
        SingleOutputStreamOperator<JSONObject> finalUserinfoDs = newUserInfoDs.filter(data -> data.containsKey("uid") && !data.getString("uid").isEmpty());
        SingleOutputStreamOperator<JSONObject> finalUserinfoSupDs = userSupMsgDs.filter(data -> data.containsKey("uid") && !data.getString("uid").isEmpty());
        //分组
        KeyedStream<JSONObject, String> keyedStreamUserInfoDs = finalUserinfoDs.keyBy(data -> data.getString("uid"));
        KeyedStream<JSONObject, String> keyedStreamUserInfoSupDs = finalUserinfoSupDs.keyBy(data -> data.getString("uid"));

        SingleOutputStreamOperator<JSONObject> processIntervalJoinUserInfo6BaseMessageDs = keyedStreamUserInfoDs.intervalJoin(keyedStreamUserInfoSupDs)
                .between(Time.minutes(-5), Time.minutes(5))
                .process(new IntervalJoinUserInfoLabelProcessFunc());

        processIntervalJoinUserInfo6BaseMessageDs.print("????");



        processIntervalJoinUserInfo6BaseMessageDs.map(data -> data.toJSONString())
                .sinkTo(
                        KafkaUtils.buildKafkaSink("cdh01:9092","base6_topic_v1")
                );
//设备
        equipmentMap.map(data -> data.toJSONString())
                .sinkTo(
                        KafkaUtils.buildKafkaSink("cdh01:9092","base2_topic_v1")
                );
//类目
        processIntervalJoinWeight.map(data -> data.toJSONString())
                .sinkTo(
                        KafkaUtils.buildKafkaSink("cdh01:9092","base4_topic_v1")
                );
        env.execute();



    }
/*
 * 计算年龄
 */
    private static int calculateAge(LocalDate birthdate,LocalDate now){
        return Period.between(birthdate,now).getYears();
    }

  //  定义星座
    private static  String constellation(LocalDate birthDate){
        //  星座  分别获取月份和日
        int month = birthDate.getMonthValue();
        int day = birthDate.getDayOfMonth();

        // 星座日期范围定义
        if ((month == 12 && day >= 22) || (month == 1 && day <= 19)) return "摩羯座";
        else if (month == 1 || month == 2 && day <= 18) return "水瓶座";
        else if (month == 2 || month == 3 && day <= 20) return "双鱼座";
        else if (month == 3 || month == 4 && day <= 19) return "白羊座";
        else if (month == 4 || month == 5 && day <= 20) return "金牛座";
        else if (month == 5 || month == 6 && day <= 21) return "双子座";
        else if (month == 6 || month == 7 && day <= 22) return "巨蟹座";
        else if (month == 7 || month == 8 && day <= 22) return "狮子座";
        else if (month == 8 || month == 9 && day <= 22) return "处女座";
        else if (month == 9 || month == 10 && day <= 23) return "天秤座";
        else if (month == 10 || month == 11 && day <= 22) return "天蝎座";
        else return "射手座";


    }


}
