package com.imooc.flink.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

public class KafkaTest {
    public static void main(String[] args) throws Exception {
        DataStreamSource<Tuple2<String, String>> stream = FlinkUtils.KafkaConsumerV3(args, DDKafkaDeserializationSchema.class);

        stream.print();

        FlinkUtils.env.execute("KafkaTest");
    }
}
