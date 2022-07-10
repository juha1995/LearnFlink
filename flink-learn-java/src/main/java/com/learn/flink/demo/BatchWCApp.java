package com.learn.flink.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 第一个基于Flink批处理快速入门案例
 */
public class BatchWCApp {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

		//从本地读取文件
		DataSource<String> source = environment.readTextFile("data/words.txt");

		source.flatMap(new WCPKFlatMapFunction())
				.map(new WCPKMapFunction())
				.groupBy(0)
				.sum(1)
				.print();

	}
}

//定义一个类实现 FlatMapFunction 实现的功能是把数据通过逗号分隔符分割并去除空格
class WCPKFlatMapFunction implements FlatMapFunction<String, String> {

	@Override
	public void flatMap(String value, Collector<String> out) throws Exception {
		String[] words = value.split(",");
		for(String word : words) {
			out.collect(word.toLowerCase().trim());
		}
	}
}

//定义一个类实现 MapFunction 实现的功能是把原数据变成一个Tuple，value和1
class WCPKMapFunction implements MapFunction<String, Tuple2<String, Integer>> {
	@Override
	public Tuple2<String, Integer> map(String value) throws Exception {
		return new Tuple2<>(value, 1);
	}
}