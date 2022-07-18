package com.learn.flink.transforms;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

public class BasicOperatorsDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 流1的数据类型都是字符串的
        SingleOutputStreamOperator<String > stream1 = env.socketTextStream("10.202.0.201", 9041).map(x->x.toString());
        //流2的数据类型都是整形的
        SingleOutputStreamOperator<Integer> stream2 = env.socketTextStream("10.202.0.201", 9042).map(x->Integer.parseInt(x));
        SingleOutputStreamOperator<String> stream3 = env.socketTextStream("10.202.0.201", 9043).map(x->x.toString());

        //union
        stream1.union(stream2.map(x->x.toString())).print(); //这里要求数据格式需要是一样的

        //connect与CoMapFunction
        ConnectedStreams<String, Integer> connect = stream1.connect(stream2);
        //第1个参数是流1的输入类型 第2个参数是流2的输入类型 第三个参数是每个流的输出类型
        connect.map(new CoMapFunction<String, Integer, String>() {

            //处理第一个流的逻辑
            @Override
            public String map1(String s) throws Exception {
                return s.toUpperCase();
            }

            //处理第二个流的逻辑
            @Override
            public String map2(Integer integer) throws Exception {
                return integer.toString();
            }
        }).print();

        //connect 与CoFlatMapFunction
        //这里的数据是根据逗号切分一行数据然后输出一个Tuple结构 value都是1
        ConnectedStreams<String, String> connect2 = stream1.connect(stream3);
        connect2.flatMap(new CoFlatMapFunction<String, String, Tuple2<String ,Integer>>() {
            @Override
            public void flatMap1(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String s1 : s.split(",")) {
                    collector.collect(new Tuple2<>(s1,1));
                }
            }

            @Override
            public void flatMap2(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String s1 : s.split(",")) {
                    collector.collect(new Tuple2<>(s1,1));
                }

            }
        }).print();

        env.execute("常用算子");
    }
}
