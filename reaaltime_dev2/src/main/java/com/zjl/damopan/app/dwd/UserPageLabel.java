package com.zjl.damopan.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.jl.constant.Constant;
import com.jl.utils.FlinkSinkUtil;
import com.jl.utils.FlinkSourceUtil;
import com.jl.utils.JdbcUtils;
import com.zjl.damopan.bean.DimBaseCategory;
import com.zjl.damopan.func.AggregateUserDataProcessFunction;
import com.zjl.damopan.func.MapDeviceAndSearchMarkModelFunc;
import com.zjl.damopan.func.MapPageInfoFacility;
import com.zjl.damopan.func.ProcessFilterRepeatTsData;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Connection;
import java.time.Duration;
import java.util.List;


public class UserPageLabel {
    private static final List<DimBaseCategory> dim_base_categories;
    private static final Connection connection;

    private static final double device_rate_weight_coefficient = 0.1; // 设备权重系数
    private static final double search_rate_weight_coefficient = 0.15; // 搜索权重系数

    static {
        try {
            connection = JdbcUtils.getMySQLConnection(
                    Constant.MYSQL_URL,
                    Constant.MYSQL_USER_NAME,
                    Constant.MYSQL_PASSWORD);
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
    @SneakyThrows
    public static void main(String[] args) {
        System.getProperty("HADOOP_USER_NAME","root");
        //todo 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 设置并行度
        env.setParallelism(1);
        //todo 设置检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //todo 获取kafka主题数据
        //todo 从 Kafka 读取 CDC 变更数据，创建一个字符串类型的数据流
        SingleOutputStreamOperator<String> kafkaSourceDs = env.fromSource(
                FlinkSourceUtil.getKafkaSource("dwd_traffic_page", "kafka_source_page_info"),
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
                "kafka_source_page_info"
        ).uid("kafka source page info").name("kafka source page info");
        kafkaSourceDs.print("kafkaSourceDs ->");
//{"common":{"ar":"25","uid":"916","os":"Android 13.0","ch":"wandoujia","is_new":"1","md":"Redmi k50","mid":"mid_365","vc":"v2.1.134","ba":"Redmi","sid":"512ff01d-43e3-4551-894a-536b975241d3"},"page":{"page_id":"good_detail","item":"34","during_time":13441,"item_type":"sku_id","last_page_id":"register"},"ts":1747304649000}

        SingleOutputStreamOperator<JSONObject> mapKafkaSourceDs = kafkaSourceDs.map(JSONObject::parseObject);
        //mapKafkaSourceDs.print("mapKafkaSourceDs ->");

        SingleOutputStreamOperator<JSONObject> mapPageInfoDs = mapKafkaSourceDs.map(new MapPageInfoFacility())
                .uid("map page info").name("kafka source page info");
        mapPageInfoDs.print("mapPageInfoDs -> ");

        SingleOutputStreamOperator<JSONObject> filterNotNullUidLogPageMsg = mapPageInfoDs.filter(data -> !data.getString("uid").isEmpty());
//        filterNotNullUidLogPageMsg.print("filterNotNullUidLogPageMsg ->");

        KeyedStream<JSONObject, String> keyedStreamLogPageMsg = filterNotNullUidLogPageMsg.keyBy(data -> data.getString("uid"));
//        keyedStreamLogPageMsg.print("keyedStreamLogPageMsg ->");

        //mapPageInfoDs -> > {"uid":"332","deviceInfo":{"ar":"31","uid":"332","os":"iOS","ch":"Appstore","md":"iPhone 14","vc":"v2.1.134","ba":"iPhone"},"ts":1744212138255}
        //mapPageInfoDs -> > {"uid":"332","deviceInfo":{"ar":"31","uid":"332","os":"iOS","ch":"Appstore","md":"iPhone 14","vc":"v2.1.134","ba":"iPhone"},"ts":1744212151506}
        //状态去重
        SingleOutputStreamOperator<JSONObject> processStagePageLogDs = keyedStreamLogPageMsg.process(new ProcessFilterRepeatTsData());
//        processStagePageLogDs.print("processStagePageLogDs ->");

        // 2 min 分钟窗口
        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = processStagePageLogDs.keyBy(data -> data.getString("uid"))
//                这个函数内部维护了每个uid的pv状态和字段集合。每次处理元素时，会更新pv和字段集合，
//                然后输出一个包含当前状态的JSON对象。这里每次处理一条记录就会输出一次，所以输出可能还是多个相同uid的记录，但pv和字段集合会逐步累积。
                .process(new AggregateUserDataProcessFunction())
                //再次按uid分区，这一步可能不是必要的，因为数据已经被第一个keyBy分区过了，但可能因为后续的窗口操作需要重新分区。
                .keyBy(data -> data.getString("uid"))

                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
//               这里使用reduce函数，每次取最新的记录。由于窗口内的数据会被合并，最终每个窗口每个uid只会保留最后一条记录。
//               导致 uid 唯一的操作
//               关键操作：窗口（Window）与归约（Reduce）的组合
                .reduce((value1, value2) -> value2)
                .uid("win 2 minutes page count msg")
                .name("win 2 minutes page count msg");
        win2MinutesPageLogsDs.print("win2MinutesPageLogsDs ->");
// 使用自定义映射函数处理win2MinutesPageLogsDs数据流，以计算设备和搜索标记模型的页面日志
        SingleOutputStreamOperator<JSONObject> deviceAndSearchMarkModelPageLogsDs = win2MinutesPageLogsDs.map(new MapDeviceAndSearchMarkModelFunc(dim_base_categories, device_rate_weight_coefficient, search_rate_weight_coefficient));
// 将处理后的页面日志转换为字符串格式，以便于后续sink到Kafka
        SingleOutputStreamOperator<String> deviceAndSearchMarkModelPageLogsDsSinkToKafka = deviceAndSearchMarkModelPageLogsDs.map(JSONObject::toString);
        // 打印处理后的页面日志数据，便于调试和验证
        deviceAndSearchMarkModelPageLogsDsSinkToKafka.print("deviceAndSearchMarkModelPageLogsDsSinkToKafka ->");
        //{"device_35_39":0.04,"os":"iOS","device_50":0.02,"search_25_29":0,"ch":"Appstore","pv":1,"device_30_34":0.05,"device_18_24":0.07,"search_50":0,"search_40_49":0,"uid":"748","device_25_29":0.06,"md":"iPhone 14","search_18_24":0,"judge_os":"iOS","search_35_39":0,"device_40_49":0.03,"search_item":"","ts_ms":"1747304573000","ba":"iPhone","search_30_34":0}


        deviceAndSearchMarkModelPageLogsDsSinkToKafka.sinkTo(FlinkSinkUtil.getKafkaSink("dwd_page_info_base_lebel"));

        env.disableOperatorChaining();
        env.execute("DbCdcPageInfoBaseLabel");
    }
}
