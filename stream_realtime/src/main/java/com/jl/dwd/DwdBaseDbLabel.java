package com.jl.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jl.utils.FlinkSinkUtil;
import com.jl.utils.FlinkSourceUtil;
import com.jl.utils.KafkaUtils;
import netscape.javascript.JSObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class DwdBaseDbLabel {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        


        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("topic_db", "one");

//        KafkaSource<String> source = kafkaSource.<String>builder()
//                .setBootstrapServers("cdh01:9092")
//                .setTopics("topic_db")
//                .setGroupId("my-group")
//                .setStartingOffsets(OffsetsInitializer.earliest())
//                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .build();
//        ------->
        final KafkaSource<String> kafkaSource1 = FlinkSourceUtil.getKafkaSource("topic_db", "one");


        DataStreamSource<String> kafkaStrDS =
                env.fromSource(kafkaSource1, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 过滤出  用户表
        SingleOutputStreamOperator<JSONObject> user_infoDS = kafkaStrDS.map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("user_info"));
        //user_infoDS.print();

        SingleOutputStreamOperator<JSONObject> map = user_infoDS.map(jsonStr -> {
            JSONObject json = JSON.parseObject(String.valueOf(jsonStr));
            JSONObject after = json.getJSONObject("after");
            if (after != null && after.containsKey("birthday")) {
                Integer epochDay = after.getInteger("birthday");
                if (epochDay != null) {
                    LocalDate date = LocalDate.ofEpochDay(epochDay);
                    after.put("birthday", date.format(DateTimeFormatter.ISO_DATE));

                    // 添加星座判断逻辑
                    String zodiacSign = getZodiacSign(date);
                    after.put("zodiac_sign", zodiacSign);

                    // 添加年代字段
                    int year = date.getYear();
                    int decade = (year / 10) * 10; // 计算年代（如1990, 2000）
                    after.put("birth_decade", decade);

                    // 添加年龄计算逻辑
                    LocalDate currentDate = LocalDate.now();
                    int age = calculateAge(date, currentDate);
                    after.put("age", age);
                }
            }
            return json;
        });
        map.print();

        //过滤 用户详情表
        SingleOutputStreamOperator<JSONObject> user_detailDS = kafkaStrDS.map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("user_info_sup_msg"));
//        user_detailDS.print();

        env.execute();
    }





    private static String getZodiacSign(LocalDate date) {
        int month = date.getMonthValue();
        int day = date.getDayOfMonth();

        // 定义星座区间映射
        if ((month == 12 && day >= 22) || (month == 1 && day <= 19)) {
            return "摩羯座";
        } else if ((month == 1 && day >= 20) || (month == 2 && day <= 18)) {
            return "水瓶座";
        } else if ((month == 2 && day >= 19) || (month == 3 && day <= 20)) {
            return "双鱼座";
        } else if ((month == 3 && day >= 21) || (month == 4 && day <= 19)) {
            return "白羊座";
        } else if ((month == 4 && day >= 20) || (month == 5 && day <= 20)) {
            return "金牛座";
        } else if ((month == 5 && day >= 21) || (month == 6 && day <= 21)) {
            return "双子座";
        } else if ((month == 6 && day >= 22) || (month == 7 && day <= 22)) {
            return "巨蟹座";
        } else if ((month == 7 && day >= 23) || (month == 8 && day <= 22)) {
            return "狮子座";
        } else if ((month == 8 && day >= 23) || (month == 9 && day <= 22)) {
            return "处女座";
        } else if ((month == 9 && day >= 23) || (month == 10 && day <= 23)) {
            return "天秤座";
        } else if ((month == 10 && day >= 24) || (month == 11 && day <= 22)) {
            return "天蝎座";
        } else if ((month == 11 && day >= 23) || (month == 12 && day <= 21)) {
            return "射手座";
        }
        return "未知"; // 默认情况，实际上不会执行到这一步
    }

    private static int calculateAge(LocalDate birthDate, LocalDate currentDate) {
        // 如果生日日期晚于当前日期，抛出异常
        if (birthDate.isAfter(currentDate)) {
            throw new IllegalArgumentException("生日日期不能晚于当前日期");
        }

        int age = currentDate.getYear() - birthDate.getYear();

        // 如果当前月份小于生日月份，或者月份相同但日期小于生日日期，则年龄减1
        if (currentDate.getMonthValue() < birthDate.getMonthValue() ||
                (currentDate.getMonthValue() == birthDate.getMonthValue() &&
                        currentDate.getDayOfMonth() < birthDate.getDayOfMonth())) {
            age--;
        }

        return age;

    }
}
