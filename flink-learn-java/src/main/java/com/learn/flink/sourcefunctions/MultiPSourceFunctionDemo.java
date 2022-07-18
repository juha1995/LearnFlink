package com.learn.flink.sourcefunctions;

import com.dao.TestLog;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;


public class MultiPSourceFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<TestLog> source = env.addSource(new MultiSourceFunctionTest())
                .setParallelism(20); // 这里设置多并行度不会有问题
        System.out.println(source.getParallelism());
        source.print();

        env.execute("自定义多并行度SourceFunction");
    }

}
class MultiSourceFunctionTest implements ParallelSourceFunction<TestLog> {
    boolean running = true;

    @Override
    public void run(SourceContext sourceContext) throws Exception {
        while (running){
            for (int i = 0; i < 10; i++) {

                TestLog testLog = new TestLog();
                testLog.setDate("2022-07-15");
                testLog.setName("Lisi");
                testLog.setUrl("www.baidu.com");
                sourceContext.collect(testLog);
            }
            Thread.sleep(5000);

        }
    }

    @Override
    public void cancel() {

    }
}
