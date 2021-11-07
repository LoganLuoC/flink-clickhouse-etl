package com.imooc.flink.basc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 第一个基于Flink批处理快速入门案例
 */
class BatchWCApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> source = env.readTextFile("data/wc.data");

        source.flatMap(new PKFlatMapFunction())
                .map(new PKMapFunction())
                .groupBy(0)
                .sum(1)
                .print();
    }
}
class PKFlatMapFunction implements FlatMapFunction<String, String> {
    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        String[] words = value.split(",");
        for (String word : words) {
            out.collect(word.toLowerCase().trim());
        }
    }
}

class PKMapFunction implements MapFunction<String, Tuple2<String, Integer>> {
    @Override
    public Tuple2<String, Integer> map(String s) throws Exception {
        return new Tuple2<>(s, 1);
    }
}