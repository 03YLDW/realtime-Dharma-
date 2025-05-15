package com.jl.Label;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jl.bean.DimBaseCategory;
import com.jl.function.*;
import com.jl.utils.FlinkSourceUtil;
import com.jl.utils.JdbcUtils;
import com.jl.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
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

public class DbusUserInfo6BaseLabelTwo {


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



        SingleOutputStreamOperator<String> kafkaCdcDbSource = env.fromSource(
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
        SingleOutputStreamOperator<String> kafkaPageLogSource = env.fromSource(
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



        SingleOutputStreamOperator<JSONObject> dataConvertJsonDs = kafkaCdcDbSource.map(JSON::parseObject)
                .uid("convert json cdc db")
                .name("convert json cdc db");

        SingleOutputStreamOperator<JSONObject> dataPageLogConvertJsonDs = kafkaPageLogSource.map(JSON::parseObject)
                .uid("convert json page log")
                .name("convert json page log");

        // 设备信息 + 关键词搜索
        SingleOutputStreamOperator<JSONObject> logDeviceInfoDs = dataPageLogConvertJsonDs.map(new MapDeviceInfoAndSearchKetWordMsgFunc())
                .uid("get device info & search")
                .name("get device info & search");


        SingleOutputStreamOperator<JSONObject> filterNotNullUidLogPageMsg = logDeviceInfoDs.filter(data -> !data.getString("uid").isEmpty());
        KeyedStream<JSONObject, String> keyedStreamLogPageMsg = filterNotNullUidLogPageMsg.keyBy(data -> data.getString("uid"));


        SingleOutputStreamOperator<JSONObject> processStagePageLogDs = keyedStreamLogPageMsg.process(new ProcessFilterRepeatTsDataFunc());

        // 2 min 分钟窗口
        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = processStagePageLogDs.keyBy(data -> data.getString("uid"))
                .process(new AggregateUserDataProcessFunction())
                .keyBy(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .reduce((value1, value2) -> value2)
                .uid("win 2 minutes page count msg")
                .name("win 2 minutes page count msg");


        // 设备打分模型 base2
        SingleOutputStreamOperator<JSONObject> mapDeviceAndSearchRateResultDs = win2MinutesPageLogsDs.map(new MapDeviceAndSearchMarkModelFunc(dim_base_categories, device_rate_weight_coefficient, search_rate_weight_coefficient));



        SingleOutputStreamOperator<JSONObject> userInfoDs = dataConvertJsonDs.filter(data -> data.getJSONObject("source").getString("table").equals("user_info"))
                .uid("filter kafka user info")
                .name("filter kafka user info");

        SingleOutputStreamOperator<JSONObject> cdcOrderInfoDs = dataConvertJsonDs.filter(data -> data.getJSONObject("source").getString("table").equals("order_info"))
                .uid("filter kafka order info")
                .name("filter kafka order info");

        SingleOutputStreamOperator<JSONObject> cdcOrderDetailDs = dataConvertJsonDs.filter(data -> data.getJSONObject("source").getString("table").equals("order_detail"))
                .uid("filter kafka order detail")
                .name("filter kafka order detail");

        SingleOutputStreamOperator<JSONObject> mapCdcOrderInfoDs = cdcOrderInfoDs.map(new MapOrderInfoDataFunc());
        SingleOutputStreamOperator<JSONObject> mapCdcOrderDetailDs = cdcOrderDetailDs.map(new MapOrderDetailFunc());

        SingleOutputStreamOperator<JSONObject> filterNotNullCdcOrderInfoDs = mapCdcOrderInfoDs.filter(data -> data.getString("id") != null && !data.getString("id").isEmpty());
        SingleOutputStreamOperator<JSONObject> filterNotNullCdcOrderDetailDs = mapCdcOrderDetailDs.filter(data -> data.getString("order_id") != null && !data.getString("order_id").isEmpty());

        KeyedStream<JSONObject, String> keyedStreamCdcOrderInfoDs = filterNotNullCdcOrderInfoDs.keyBy(data -> data.getString("id"));
        KeyedStream<JSONObject, String> keyedStreamCdcOrderDetailDs = filterNotNullCdcOrderDetailDs.keyBy(data -> data.getString("order_id"));

        SingleOutputStreamOperator<JSONObject> processIntervalJoinOrderInfoAndDetailDs = keyedStreamCdcOrderInfoDs.intervalJoin(keyedStreamCdcOrderDetailDs)
                .between(Time.minutes(-2), Time.minutes(2))
                .process(new IntervalDbOrderInfoJoinOrderDetailProcessFunc());

        SingleOutputStreamOperator<JSONObject> processDuplicateOrderInfoAndDetailDs = processIntervalJoinOrderInfoAndDetailDs.keyBy(data -> data.getString("detail_id"))
                .process(new processOrderInfoAndDetailFunc());

        //todo 品类 品牌 年龄 时间 base4
        SingleOutputStreamOperator<JSONObject> mapOrderInfoAndDetailModelDs = processDuplicateOrderInfoAndDetailDs.map(new MapOrderAndDetailRateModelFunc(dim_base_categories, time_rate_weight_coefficient, amount_rate_weight_coefficient, brand_rate_weight_coefficient, category_rate_weight_coefficient));


        mapOrderInfoAndDetailModelDs.print("品类 品牌 年龄 时间 base4");
        //{"payment_way":"3501","b1name_25-29":0.12,"tname_30-34":0.14,"amount_18-24":0.015,"amount_30-34":0.045,"b1name_50":0.21,"order_status":"1002","tname_18-24":0.18,"tname_40-49":0.14,"b1_name":"家用电器","b1name_30-34":0.18,"b1name_40-49":0.27,"trade_body":"华为智慧屏V65i 65英寸 HEGE-560B 4K全面屏智能电视机 多方视频通话 AI升降摄像头 4GB+32GB 星际黑等2件商品","tname_25-29":0.16,"id":"2031","b1name_18-24":0.06,"consignee":"夏侯亮","create_time":"1744133348000","c3id":"86","tname_35-39":0.14,"tname":"联想","sku_id":"35","amount_35-39":0.06,"amount_50":0.09,"tname_50":0.1,"b1name_35-39":0.24,"amount_25-29":0.03,"total_amount":10998.0,"user_id":"379","province_id":"14","amount_40-49":0.075,"ts_ms":"1747055322307","split_total_amount":"10998.0"}



        SingleOutputStreamOperator<JSONObject> userinfoBD = userInfoDs.map(new MapFunction<JSONObject, JSONObject>() {
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
        SingleOutputStreamOperator<JSONObject> user_info_sup_msg = dataConvertJsonDs.filter(data -> data.getJSONObject("source").getString("table").equals("user_info_sup_msg"));
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
        // TODO: 2025/5/15  六大基础特征
        SingleOutputStreamOperator<JSONObject> processIntervalJoinUserInfo6BaseMessageDs = keyedStreamUserInfoDs.intervalJoin(keyedStreamUserInfoSupDs)
                .between(Time.minutes(-5), Time.minutes(5))
                .process(new IntervalJoinUserInfoLabelProcessFunc());

//        processIntervalJoinUserInfo6BaseMessageDs.print();

        mapOrderInfoAndDetailModelDs.map(data->data.toJSONString()).sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092", "base4_topic"));
        win2MinutesPageLogsDs.map(data->data.toJSONString()).sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092", "base2_topic"));
        processIntervalJoinUserInfo6BaseMessageDs.map(data->data.toJSONString()).sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092", "base6_topic"));




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
