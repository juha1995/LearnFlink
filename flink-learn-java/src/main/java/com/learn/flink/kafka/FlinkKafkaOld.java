package com.learn.flink.kafka;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

/**
 * 这里的Kafka的api是老版本的
 */
public class FlinkKafkaOld {
//    public static void main(String[] args) {
//
//    }
//
//    public static void fromKafka(StreamExecutionEnvironment env){
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers","kafka01:9092");
//        properties.setProperty("group.id","test");
//        DataStreamSource<Object> source = env.addSource(new FlinkKafkaConsumer<Object>("topic",new SimpleStringSchema(),properties));
//    }
}
