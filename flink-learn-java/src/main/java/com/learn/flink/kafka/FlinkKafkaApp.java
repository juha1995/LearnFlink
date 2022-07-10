package com.learn.flink.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class FlinkKafkaApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers","kafka1:9092,kafka2:9092,kafka3:9092");
//        properties.set("group.id","test_0515");
//
//        DataStreamSource<OUT> test = env.addSource(new FlinkKafkaConsumer<>("test",new SimpleStringSchema()));



        String brokers = "kafka1:9092,kafka2:9092,kafka3:9092";

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics("test")
                .setGroupId("my-test_0515")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        KafkaSource<String> test1 = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics("test")
                .setGroupId("my-test_0515")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class)).build();
        System.out.println(source);
        System.out.println(test1);

//        SourceReader<String, KafkaPartitionSplit> reader = source.createReader();

        DataStream<String> stream = env.readTextFile("data/test.txt");
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("test")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        stream.sinkTo(sink);
        env.execute("Test Kafka");
    }
}
