package com.imooc.flink.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class FlinkKafkaApp {
    public static void main(String[] args) throws Exception {

        DataStream<String> stream = FlinkUtils.KafkaConsumerV2(args, SimpleStringSchema.class);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = streamOperate(stream);

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();

        result.addSink(new RedisSink<Tuple2<String, Integer>>(conf, new RedisExampleMapper()));

        FlinkUtils.env.execute("FlinkKafkaApp");

    }

    private static SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperate(DataStream<String> stream) {
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = stream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] lines = value.split(" ");
                for (String word : lines) {
                    out.collect(word);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2 map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        }).keyBy(x -> x.f0)
                .sum(1);
        return result;
    }

    public static class RedisExampleMapper implements RedisMapper<Tuple2<String, Integer>>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "wc-cnt:2021-11-01");
        }

        @Override
        public String getKeyFromData(Tuple2<String, Integer> value) {
            return value.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Integer> value) {
            return "" + value.f1;
        }
    }

    public static void test() throws Exception {
        /**
         * bootstrap.servers=localhost:9092
         * group.id=test
         * topic=topic_test
         * enable.auto.commit=false
         * auto.offset.reset=earliest
         *
         * checkpoint.Interval=5000
         * checkpoint.fs=file:///Users/carves/Documents/projects/flink-learn/state
         */

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka 配置参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        properties.setProperty("enable.auto.commit", "false");
        properties.setProperty("auto.offset.reset", "earliest");
        String topic = "topic_test";

        // checkpoint 参数，可以在配置文件中修改
        env.enableCheckpointing(5000);
        env.setStateBackend(new FsStateBackend("file:///Users/carves/Documents/projects/flink-learn/state"));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.of(5, TimeUnit.SECONDS)));

        // consumer 配置
        FlinkKafkaConsumer<String> myConsumer =
                new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);

        myConsumer.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .forBoundedOutOfOrderness(Duration.ofSeconds(20)));

        DataStream<String> stream = env.addSource(myConsumer);
        stream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split(" ");
                for (String s : split) {
                    out.collect(s);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        }).keyBy(x -> x.f0)
                .sum(1)
                .print("wc 统计");

        env.socketTextStream("localhost", 9527)
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        if (value.equals("dd")) throw new RuntimeException("dd 淘气了");
                        return value.toUpperCase();
                    }
                }).print(" socket 统计");

        env.execute("FlinkKafkaApp");
    }
}
