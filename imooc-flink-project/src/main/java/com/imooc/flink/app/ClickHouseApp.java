package com.imooc.flink.app;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;


public class ClickHouseApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Tuple3<String, String, String>> result = env.socketTextStream("localhost", 9527)
                .map(getMapper());

        result.print();
        result.addSink(getSink());

        env.execute("ClickHouseApp");
    }

    private static MapFunction<String, Tuple3<String, String, String>> getMapper() {
        return new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] line = value.split(",");
                return Tuple3.of(line[0].trim(), line[1].trim(), line[2].trim());
            }
        };
    }

    private static SinkFunction<Tuple3<String, String, String>> getSink() {
        return JdbcSink.sink("insert into ch_test values (?, ? ,?)",
                (pstmt, x) -> {
                    pstmt.setString(1, x.f0);
                    pstmt.setString(2, x.f1);
                    pstmt.setString(3, x.f2);
                },
                JdbcExecutionOptions.builder().withBatchSize(3).withBatchIntervalMs(1000).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://localhost:8123/dd")
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .build()
        );
    }
}
