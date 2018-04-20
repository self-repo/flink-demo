package com.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public final class Tokenizer implements FlatMapFunction<String, Info> {

	private static final long serialVersionUID = 1L;

	public void flatMap(String value, Collector<Info> out) {
		out.collect(new Info(value));
	}

}