package com.imooc.flink.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();

        test01(env);

        env.execute("WindowApp");
    }

    public static void test01(StreamExecutionEnvironment env) {

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        env.socketTextStream("localhost", 9527)
                .map(new MapFunction<String, Integer>() {
                    @Override
                    public Integer map(String value) throws Exception {
                        return Integer.parseInt(value);
                    }
                }).timeWindowAll(Time.seconds(5))
                .sum(0)
                .print();
    }
}
