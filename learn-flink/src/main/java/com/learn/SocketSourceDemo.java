package com.learn;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;

public class SocketSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//         这里的10.202.0.201是另一台Linux环境的机器
        DataStreamSource<String> source = env.socketTextStream("10.202.0.201",9394);
        System.out.println("socketTextStream并行度为"+source.getParallelism());

        //手动调整一下并行度 此时会报错，因为socketTextStream 并行度只可以是1
        source.setParallelism(20);


        SingleOutputStreamOperator<String> filterString = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return !"haha".equals(s); // 过滤掉haha的字符串
            }
        });
        //默认的并行度是cpu的核数
        System.out.println("filterString的并行度为"+filterString.getParallelism());

        //可以手动调整算子的并行度
        filterString.setParallelism(20);
        System.out.println("filterString的并行度为"+filterString.getParallelism());

        filterString.print();

        env.execute("SocketSourceDemo");
    }

}
