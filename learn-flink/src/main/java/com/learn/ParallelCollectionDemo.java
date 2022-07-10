package com.learn;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;

public class ParallelCollectionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        fromParallelCollectionDemo(env);

    }

    public  static  void fromParallelCollectionDemo(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<Long> source = env.fromParallelCollection(
                new NumberSequenceIterator(1,10),Long.class
        );
        System.out.println("fromParallelCollection并行度为---"+source.getParallelism());

        //这里的并行度是可以调整的
        source.setParallelism(20);
        System.out.println("修改后的fromParallelCollection并行度为---"+source.getParallelism());


        SingleOutputStreamOperator<Long> filter = source.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long aLong) throws Exception {
                return aLong >= 5;
            }
        });

        System.out.println("filter的并行度为---"+filter.getParallelism());

        env.execute("fromParallelCollection");
    }
}
