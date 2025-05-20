package com.zjl.damopan.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.jl.utils.KafkaUtils;
import com.zjl.damopan.utils.ProcessJoinBase2And4BaseFunc;
import com.zjl.damopan.utils.ProcessLabelFunc;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Date;


public class DbusUserLabel6BaseCalculate {
    private static final String kafka_botstrap_servers = "cdh01:9092";
    private static final String kafka_label_base6_topic = "dwd_base6_label";
    private static final String kafka_label_base4_topic = "dwd_order_info_base_label";
    private static final String kafka_label_base2_topic = "dwd_page_info_base_lebel";

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettingUtils.defaultParameter(env);
// 从Kafka中构建一个数据流源，用于处理特定主题的实时数据
        SingleOutputStreamOperator<String> kafkaBase6Source = env.fromSource(
                        // 使用Kafka工具类构建一个安全的Kafka源
                        KafkaUtils.buildKafkaSecureSource(
                                kafka_botstrap_servers, // Kafka服务器地址
                                kafka_label_base6_topic, // Kafka主题
                                new Date().toString(),// 从当前时间开始读取数据
                                OffsetsInitializer.earliest()// 从最早的偏移量开始读取
                        ),
                        // 配置水印策略，处理无序事件
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((event, timestamp) -> {
                                    // 解析事件中的时间戳，用于水印生成
                                            JSONObject jsonObject = JSONObject.parseObject(event);
                                            if (event != null && jsonObject.containsKey("ts_ms")){
                                                try {
                                                    // 如果事件中包含ts_ms字段，则使用该字段作为事件时间戳
                                                    return JSONObject.parseObject(event).getLong("ts_ms");
                                                }catch (Exception e){
                                                    // 异常处理：打印错误信息并返回0作为默认时间戳
                                                    e.printStackTrace();
                                                    System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                                    return 0L;
                                                }
                                            }
                                    // 如果事件中不包含ts_ms字段，则返回0作为默认时间戳
                                            return 0L;
                                        }
                                ),
                        "kafka_label_base6_topic_source"
                ).uid("kafka_base6_source")
                .name("kafka_base6_source");

        SingleOutputStreamOperator<String> kafkaBase4Source = env.fromSource(
                        KafkaUtils.buildKafkaSecureSource(
                                kafka_botstrap_servers,
                                kafka_label_base4_topic,
                                new Date().toString(),
                                OffsetsInitializer.earliest()
                        ),
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((event, timestamp) -> {
                                            JSONObject jsonObject = JSONObject.parseObject(event);
                                            if (event != null && jsonObject.containsKey("ts_ms")){
                                                try {
                                                    return JSONObject.parseObject(event).getLong("ts_ms");
                                                }catch (Exception e){
                                                    e.printStackTrace();
                                                    System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                                    return 0L;
                                                }
                                            }
                                            return 0L;
                                        }
                                ),
                        "kafka_label_base4_topic_source"
                ).uid("kafka_base4_source")
                .name("kafka_base4_source");

        SingleOutputStreamOperator<String> kafkaBase2Source = env.fromSource(
                        KafkaUtils.buildKafkaSecureSource(
                                kafka_botstrap_servers,
                                kafka_label_base2_topic,
                                new Date().toString(),
                                OffsetsInitializer.earliest()
                        ),
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((event, timestamp) -> {
                                            JSONObject jsonObject = JSONObject.parseObject(event);
                                            if (event != null && jsonObject.containsKey("ts_ms")){
                                                try {
                                                    return JSONObject.parseObject(event).getLong("ts_ms");
                                                }catch (Exception e){
                                                    e.printStackTrace();
                                                    System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                                    return 0L;
                                                }
                                            }
                                            return 0L;
                                        }
                                ),
                        "kafka_label_base2_topic_source"
                ).uid("kafka_base2_source")
                .name("kafka_base2_source");

        SingleOutputStreamOperator<JSONObject> mapBase6LabelDs = kafkaBase6Source.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> mapBase4LabelDs = kafkaBase4Source.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> mapBase2LabelDs = kafkaBase2Source.map(JSON::parseObject);


        SingleOutputStreamOperator<JSONObject> join2_4Ds = mapBase2LabelDs.keyBy(o -> o.getString("uid"))
                //        基于时间区间的双流连接操作
                .intervalJoin(mapBase4LabelDs.keyBy(o -> o.getString("user_id")))
                .between(Time.days(-5), Time.days(5))
                .process(new ProcessJoinBase2And4BaseFunc());
// 使用WatermarkStrategy为join2_4Ds数据流分配时间戳和水印
// 设置最大允许的乱序时间为5秒
// 指定时间戳的获取方式：从JSONObject中提取"ts_ms"字段作为时间戳
        SingleOutputStreamOperator<JSONObject> waterJoin2_4 = join2_4Ds.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (jsonObject, l) -> jsonObject.getLongValue("ts_ms")));

        SingleOutputStreamOperator<JSONObject> userLabelProcessDs = waterJoin2_4
                .keyBy(o -> o.getString("uid"))
                .intervalJoin(mapBase6LabelDs.keyBy(o -> o.getString("uid")))
                .between(Time.days(-5), Time.days(5))
                .process(new ProcessLabelFunc());

//        join2_4Ds.print();
        userLabelProcessDs.print();
//        userLabelProcessDs.writeAsText("E:\\csv/output.csv").setParallelism(1);

        env.execute();
    }

}
