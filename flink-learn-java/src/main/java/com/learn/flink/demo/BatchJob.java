/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.learn.flink.demo;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批处理程序
 */
public class BatchJob {

	public static void main(String[] args) throws Exception {

		//获取环境
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


		//source数据源
		DataSource<String> source = env.readTextFile("H:\\Coding\\flink-train-java\\flink-learn-java\\data\\words.txt");


		//执行操作 transformation
		source.flatMap(new PKFlatMapFunction() )
				.filter(new PKFilterFunction() )
				.map(new PKMapFunction())
				.groupBy(0)
				.sum(1)
				.print();
	}
}

 class PKFlatMapFunction implements FlatMapFunction<String ,String >{
	@Override
	public void flatMap(String value, Collector<String> out) throws Exception {
			String[] words = value.split(",");
			for(String word : words) {
			out.collect(word.toLowerCase().trim());
				}
	}
}

class PKFilterFunction implements FilterFunction<String >{
	@Override
	public boolean filter(String value) throws Exception {
		return StringUtils.isNotEmpty(value);
	}
}
class PKMapFunction implements MapFunction<String, Tuple2<String, Integer>>{
	@Override
	public Tuple2<String, Integer> map(String value) throws Exception {
		return new Tuple2<>(value, 1);
	}
}