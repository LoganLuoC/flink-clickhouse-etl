package com.imooc.flink.job;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class KafkaJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.readTextFile("wc.data").getParallelism();

        getKafka(env);

        env.execute();
    }

    /**
     * 业务需求：
     * 1。 从kafka flink-topic中读取数据
     * 2。 将数据按照逗号进行word count统计，并以：分割写入kafka
     * 3。 写入kafka word-count topic中
     * @param env
     */
    public static void getKafka(StreamExecutionEnvironment env) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("flink-topic", new SimpleStringSchema(), properties).setStartFromEarliest());

        Properties sinkProperties = new Properties();
        sinkProperties.setProperty("bootstrap.servers", "localhost:9092");

        FlinkKafkaProducer myProducer = new FlinkKafkaProducer<>(
                "word-count",                  // target topic
                new SimpleStringSchema(),    // serialization schema
                properties); // fault-tolerance

        stream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] splits = s.split(",");
                for (String word : splits) {
                    collector.collect(word.trim());
                }
            }
        })
        .map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        })
        .keyBy(x -> x.f0).sum(1)
        .map(new MapFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return "" + stringIntegerTuple2.f0 + " : " + stringIntegerTuple2.f1;
            }
        })
        .addSink(myProducer);

    }
}
