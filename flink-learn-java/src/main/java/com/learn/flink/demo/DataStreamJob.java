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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


		//source数据源
		DataStreamSource<String> source = env.socketTextStream("10.202.0.201",9394);



		//执行操作 transformation
		// FlatmapFunction  两个参数，第一个是输入，第二个是输出
		source.flatMap(new SplitStringFunc())
				.filter(new EmptyFilterFunction())
				.map(new AddOneMapFunction() )
				.keyBy(new KeyByFunction())
				.sum(1)
				.print();


		// 一定要有，不然不会执行
		env.execute("Flink Java API Skeleton");
	}
}

class SplitStringFunc implements FlatMapFunction<String ,String >{
	@Override
	public void flatMap(String value, Collector<String> out) throws Exception {
		String[] words = value.split(",");
		for(String word : words) {
			out.collect(word.toLowerCase().trim());
		}
	}
}


class EmptyFilterFunction implements FilterFunction<String >{
	@Override
	public boolean filter(String value) throws Exception {
		return StringUtils.isNotEmpty(value);
	}
}

/**
 * 把 原来的字符串后面加上一个1，变成kv结构
 */
class AddOneMapFunction implements MapFunction<String, Tuple2<String, Integer>>{
	@Override
	public Tuple2<String, Integer> map(String value) throws Exception {
		return new Tuple2<>(value, 1);
	}
}

class KeyByFunction implements KeySelector<Tuple2<String,Integer>, String>{
	@Override
	public String getKey(Tuple2<String, Integer> value) throws Exception {
		return value.f0;
	}
}
