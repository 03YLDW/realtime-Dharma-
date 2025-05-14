package com.jl.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jl.bean.DimBaseCategory;
import com.jl.function.*;
import com.jl.utils.ConfigUtils;
import com.jl.utils.JdbcUtils;
import com.jl.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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

    static {
        try {
            connection = JdbcUtils.getMySQLConnection(
                    "jdbc:mysql://cdh03:3306",
                    "root",
                        "root");
            String sql = "select b3.id,                          \n" +
                    "            b3.name as b3name,              \n" +
                    "            b2.name as b2name,              \n" +
                    "            b1.name as b1name               \n" +
                    "     from realtime_v1.base_category3 as b3  \n" +
                    "     join realtime_v1.base_category2 as b2  \n" +
                    "     on b3.category2_id = b2.id             \n" +
                    "     join realtime_v1.base_category1 as b1  \n" +
                    "     on b2.category1_id = b1.id";
            dim_base_categories = JdbcUtils.queryList2(connection, sql, DimBaseCategory.class, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }







    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//topic_db数据
//        {
//            "before": null,
//                "after": {
//            "id": 2,
//                    "activity_id": 1,
//                    "activity_type": "3101",
//                    "condition_amount": 8000.0,
//                    "condition_num": null,
//                    "benefit_amount": 900.0,
//                    "benefit_discount": null,
//                    "benefit_level": 2,
//                    "create_time": 1644714512000,
//                    "operate_time": null
//        },
//            "source": {
//            "version": "1.9.7.Final",
//                    "connector": "mysql",
//                    "name": "mysql_binlog_source",
//                    "ts_ms": 1744098486000,
//                    "snapshot": "false",
//                    "db": "realtime_v1",
//                    "sequence": null,
//                    "table": "activity_rule",
//                    "server_id": 1,
//                    "gtid": null,
//                    "file": "mysql-bin.000001",
//                    "pos": 794430,
//                    "row": 0,
//                    "thread": 129,
//                    "query": null
//        },
//            "op": "c",
//                "ts_ms": 1747055319968,
//                "transaction": null
//        }


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
                                //若解析失败（如字段缺失或格式错误），打印错误日志并返回 0L（需注意这可能引发后续计算问题）。
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

//        kfk_cdc_source.print();

        //读取topic_log
//        {
//            "common": {
//            "ar": "1",
//                    "ba": "iPhone",
//                    "ch": "Appstore",
//                    "is_new": "0",
//                    "md": "iPhone 14",
//                    "mid": "mid_389",
//                    "os": "iOS 13.3.1",
//                    "sid": "93171837-27ca-47f1-880e-62f08fa622f6",
//                    "vc": "v2.1.134"
//        },
//            "start": {
//            "entry": "icon",
//                    "loading_time": 14804,
//                    "open_ad_id": 8,
//                    "open_ad_ms": 3561,
//                    "open_ad_skip_ms": 0
//        },
//            "ts": 1744104535000
//        }


//        kfk_cdc_source.print();
//          将数据转换成 JSONObject                                            JSONObject.parseObject(data) <====> JSON::parseObject
//        读取 日志数据   topic_log
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


        // 设备信息 + 关键词搜索
        SingleOutputStreamOperator<JSONObject> logDeviceInfoDs = dataPageLogConvertJsonDs.map(new MapDeviceInfoAndSearchKetWordMsgFunc())
                .uid("get device info & search")
                .name("get device info & search");

//        logDeviceInfoDs.print();

        SingleOutputStreamOperator<JSONObject> kfk_source = kfk_cdc_source.map(JSONObject::parseObject)
                .uid("parseObject")
                .name("parseObject");
        // 过滤出 user_info
        SingleOutputStreamOperator<JSONObject> userinfoDs = kfk_source.filter(data -> data.getJSONObject("source").getString("table").equals("user_info"))
                .uid("userinfoDs")
                .name("userinfoDs");

        SingleOutputStreamOperator<JSONObject> filterNotNullUidLogPageMsg = logDeviceInfoDs.filter(data -> !data.getString("uid").isEmpty());
        KeyedStream<JSONObject, String> keyedStreamLogPageMsg = filterNotNullUidLogPageMsg.keyBy(data -> data.getString("uid"));

        SingleOutputStreamOperator<JSONObject> processStagePageLogDs = keyedStreamLogPageMsg.process(new ProcessFilterRepeatTsDataFunc());

        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = processStagePageLogDs.keyBy(data -> data.getString("uid"))
                .process(new AggregateUserDataProcessFunction())
                .keyBy(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .reduce((value1, value2) -> value2)
                .uid("win 2 minutes page count msg")
                .name("win 2 minutes page count msg");

//        win2MinutesPageLogsDs.print();
//{"uid":"470","os":"Android","ch":"xiaomi","pv":4,"md":"vivo IQOO Z6x ","search_item":"心相印纸抽","ba":"vivo"}


        // 设备打分模型
        win2MinutesPageLogsDs.map(new MapDeviceAndSearchMarkModelFunc(dim_base_categories,device_rate_weight_coefficient,search_rate_weight_coefficient))
                .print();
//{"device_35_39":0.04,"os":"iOS","device_50":0.02,"search_25_29":0,"ch":"Appstore","pv":1,"device_30_34":0.05,"device_18_24":0.07,"search_50":0,"search_40_49":0,"uid":"249","device_25_29":0.06,"md":"iPhone 14 Plus","search_18_24":0,"judge_os":"iOS","search_35_39":0,"device_40_49":0.03,"search_item":"","ba":"iPhone","search_30_34":0}



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

        KeyedStream<JSONObject, String> keyedStreamUserInfoDs = finalUserinfoDs.keyBy(data -> data.getString("uid"));
        KeyedStream<JSONObject, String> keyedStreamUserInfoSupDs = finalUserinfoSupDs.keyBy(data -> data.getString("uid"));

        SingleOutputStreamOperator<JSONObject> processIntervalJoinUserInfo6BaseMessageDs = keyedStreamUserInfoDs.intervalJoin(keyedStreamUserInfoSupDs)
                .between(Time.minutes(-5), Time.minutes(5))
                .process(new IntervalJoinUserInfoLabelProcessFunc());

//        processIntervalJoinUserInfo6BaseMessageDs.print();

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
