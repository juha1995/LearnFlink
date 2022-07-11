package com.learn.flink.sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("10.202.0.201", 9394);

        source.print().setParallelism(10);
        System.out.println(source.getParallelism());

        env.execute("SinkDemo");
    }
}
