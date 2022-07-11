package com.learn.flink.richfunctions;

import com.dao.TestLog;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapRichFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.readTextFile("data/test.log");
        source.map(new MyRichMapFunction());
        env.execute("MapRichFunctionDemo");

    }
}

class MyRichMapFunction extends RichMapFunction<String , TestLog>{


    /**
     * 业务逻辑
     * 这里的testlog是一个类，里面有4个元素 date、url、name还有一个cnt计数
     * @param s
     * @return
     * @throws Exception
     */
    @Override
    public TestLog map(String s) throws Exception {
        System.out.println("我是map");

        String[] splits = s.split(",");
        String date = splits[0].trim();
        String url = splits[1].trim();
        String name = splits[2].trim();

        return new TestLog(date,url,name,1);
    }

    /**
     * 初始化操作
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("我是open");
        super.open(parameters);
    }


    /**
     * 关闭资源操纵
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        System.out.println("我是close");
        super.close();
    }


}