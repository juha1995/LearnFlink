package com.learn.flink.java.quickstart;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

public class BatchECJavaApp {
    public static void main(String[] args) throws Exception {
        String input = "H:\\Coding\\flink-train-java";

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //读取目标路径下数据,会创建一个数据集
        DataSource<String> text = env.readTextFile(input);


        text.print();
    }
}
