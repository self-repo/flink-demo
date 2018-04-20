package com.demo;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by david03.wang on 2018/1/18.
 */
public class CepMain {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Price> input = env.fromElements(new Price("mk", 1000l), new Price("cache", 800l),
                new Price("jackjson", 700));

        Pattern<Price, ?> pattern = Pattern.<Price>begin("filter-001").where(new IterativeCondition<Price>() {
            @Override
            public boolean filter(Price value, Context<Price> ctx) throws Exception {
                return value.getPrice() > 100;
            }
        }).followedBy("filter-002").where(new IterativeCondition<Price>() {
            @Override
            public boolean filter(Price value, Context<Price> ctx) throws Exception {
                return value.getName().equals("mk") || value.getName().equals("cache");
            }
        });

        DataStream<Double> result = CEP.pattern(input, pattern).select(new PatternSelectFunction<Price, Double>() {
            public Double select(Map<String, List<Price>> pattern) throws Exception {
                Collection<List<Price>> collection = pattern.values();
                double sum = 0.0;
                if (null != collection) {
                    for (List<Price> tList : collection) {
                        if (null != tList) {
                            for (Price p : tList) {
                                sum += p.getPrice();
                            }
                        }
                    }
                }
                return sum;
            }
        });

        result.print();

        try {
            env.execute("cep-demo");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
