package com.learn.flink.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.StringDeserializer;


public class FlinkKafkaApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //kafka的环境信息
        String brokers = "kafka1:9092,kafka2:9092,kafka3:9092";

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics("poc_rem_xsht.bfmall10.xsht_part_2022")
                .setGroupId("my-test_0515")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .build();



        DataStreamSource<String> kafka_source = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");



        System.out.println("kafka_source并行度为"+kafka_source.getParallelism());

        //打印从Kafka读取到的数据
        kafka_source.print();

//        SingleOutputStreamOperator<String> operator = kafka_source.flatMap(new SplitStringFunc());
//        operator.print();

        env.execute("Test Kafka");
    }
}

/**
 * 将文字通过逗号分隔
 * TODO...
 */
class SplitStringFunc implements FlatMapFunction <String ,String >{
    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        String[] words = value.split(",");
        for(String word : words) {
            out.collect(word.toLowerCase().trim());
        }
    }
}
