package com.imooc.flink.monitor;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MonitorApp {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Long>> tuple2DataStreamSource = env.fromElements(
                Tuple2.of("a", 50L), Tuple2.of("a", 80L), Tuple2.of("a", 100L),
                Tuple2.of("a", 40L), Tuple2.of("a", 90L), Tuple2.of("a", 110L),
                Tuple2.of("a", 60L), Tuple2.of("a", 20L), Tuple2.of("a", 200L),
                Tuple2.of("b", 5L), Tuple2.of("b", 30L), Tuple2.of("b", 100L),
                Tuple2.of("b", 220L), Tuple2.of("b", 330L), Tuple2.of("b", 400L),
                Tuple2.of("b", 340L), Tuple2.of("b", 400L), Tuple2.of("b", 200L)
        );
        tuple2DataStreamSource
                .keyBy(0)
//                .flatMap(new ThresholdWarning(100L , 3))
                .printToErr();
//        env.execute();
    }
}
