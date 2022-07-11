package com.learn.flink.demo;

import com.dao.TestLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DemoFromLog {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("data/test.log");
//        source.map(new AddOneMapFunction()).print();

        SingleOutputStreamOperator<TestLog> map = source.map(new AddSchema());
        map.keyBy(x->x.getName()).sum("cnt").print();
        env.execute("给予文件Schema信息");
    }
}

/**
 * 给文件添加schema信息 日期 网址 姓名 cnt计数
 * 这里注意，schema需要有无参构造
 */
class AddSchema implements MapFunction <String ,  TestLog> {
    @Override
    public TestLog map(String s) throws Exception {
        String[] splits = s.split(",");
        String date = splits[0].trim();
        String url = splits[1].trim();
        String name = splits[2].trim();

        return new TestLog(date,url,name,1);
    }
}
