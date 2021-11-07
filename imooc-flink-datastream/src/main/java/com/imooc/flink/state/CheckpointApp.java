package com.imooc.flink.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Int;

import java.util.concurrent.TimeUnit;

public class CheckpointApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启checkpoint
        /**
         * 不开启checkpoint: 不重启
         * 配置了重启策略： 使用配置的重启策略
         * 1. 使用默认的重启策略: Integer.MAX_VALUE
         * 2. 配置了重启策略， 使用配置的重启策略覆盖默认的
         *
         * 重启策略的配置：
         * 1. code
         * 2. yaml
         */
        env.enableCheckpointing(5000);
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        // 作业完成后是否保留
        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置状态后端
        config.setCheckpointStorage("file:////Users/carves/workspace/imook-flink");

        // 自定义设置我们需要的重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // number of restart attempts, 正常运行之后，进入错误再运行的次数
                Time.of(10, TimeUnit.SECONDS) // delay
        ));

        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);
        source.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                if (value.contains("pk")) {
                    throw new RuntimeException("PK pk哥");
                } else {
                    return value.toLowerCase();
                }
            }
        }).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] splits = value.split(",");
                for (String split:
                     splits) {
                    out.collect(split);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        }).keyBy(value -> value.f0)
                .sum(1)
                .print();

//        source.print();

        env.execute("CheckpointApp");
    }
}
