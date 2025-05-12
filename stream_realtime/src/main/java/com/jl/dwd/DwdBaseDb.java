package com.jl.dwd;

import com.jl.utils.FlinkSinkUtil;
import com.jl.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DwdBaseDb {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        env.enableCheckpointing();

        final KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("", "");

        final DataStreamSource<String> kafkaData = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        env.execute();



    }
}
