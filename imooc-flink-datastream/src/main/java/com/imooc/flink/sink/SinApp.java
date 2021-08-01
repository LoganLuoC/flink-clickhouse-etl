package com.imooc.flink.sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SinApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);

        System.out.println("source..." + source.getParallelism());

        source.print("prefix").setParallelism(2);

        env.execute("SinkApp");
    }
}
