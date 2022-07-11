package com.learn;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class NormalCollectionDemo {

    public static void main(String[] args) throws Exception {

        //创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //创建一个数组
        List<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(4);
        list.add(6);

        //数组的值都乘以2
        DataStreamSource<Integer> source = env.fromCollection(list);

        source.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer integer) throws Exception {
                return integer*2;
            }
        }).print();

        env.execute("NormalCollection");
    }
}
