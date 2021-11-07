package com.imooc.flink.app;

import com.alibaba.fastjson.JSON;
import com.imooc.flink.domian.AccessV2;
import com.imooc.flink.kafka.DDKafkaDeserializationSchema;
import com.imooc.flink.kafka.FlinkUtils;
import com.imooc.flink.udf.GaodeLocationV2;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class FlinkETLApp {
    public static void main(String[] args) throws Exception {

        DataStreamSource<Tuple2<String, String>> stream = FlinkUtils.KafkaConsumerV3(args, DDKafkaDeserializationSchema.class);

        FastDateFormat instance = FastDateFormat.getInstance("yyyyMMdd-HH");

        stream.map(new MapFunction<Tuple2<String, String>, AccessV2>() {

            @Override
            public AccessV2 map(Tuple2<String, String> value) throws Exception {
                // json ==> 自定义对象
                try {
                    AccessV2 bean = JSON.parseObject(value.f1, AccessV2.class);
                    bean.id = value.f0;
                    String[] strings = instance.format(bean.time).split("-");
                    bean.date = strings[0];
                    bean.hour = strings[1];
                    return bean;

                } catch (Exception e) {
                    e.printStackTrace();
                    // TODO... 把这些异常的数据记录到某个地方去
                    return null;
                }
            }
        })
        .filter( x -> x != null)
        .filter(new FilterFunction<AccessV2>() {
            @Override
            public boolean filter(AccessV2 value) throws Exception {
                return "startup".equals(value.event);
            }
        })
                .map(new GaodeLocationV2()).addSink(getSinkV2());

        FlinkUtils.env.execute("");
    }


    private static SinkFunction<AccessV2> getSinkV2() {
        return JdbcSink.sink("insert into ch_event values (?, ? ,?, ?, ? ,?, ?, ? ,?, ?, ? ,?, ?, ? ,?, ?, ? ,?)",
                (pstmt, x) -> {
                    pstmt.setString(1, x.id);
                    pstmt.setString(2, x.device);
                    pstmt.setString(3, x.deviceType);
                    pstmt.setString(4, x.os);
                    pstmt.setString(5, x.event);
                    pstmt.setString(6, x.net);
                    pstmt.setString(7, x.channel);
                    pstmt.setString(8, x.uid);
                    pstmt.setInt(9, x.nu);
                    pstmt.setInt(10, x.nu2);
                    pstmt.setString(11, x.ip);
                    pstmt.setLong(12, x.time);
                    pstmt.setString(13, x.version);
                    pstmt.setString(14, x.province);
                    pstmt.setString(15, x.city);
                    pstmt.setString(16, x.date);
                    pstmt.setString(17, x.hour);
                    pstmt.setLong(18, System.currentTimeMillis());

                },
                JdbcExecutionOptions.builder().withBatchSize(50).withBatchIntervalMs(1000).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://localhost:8123/dd")
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .build()
        );
    }
}
