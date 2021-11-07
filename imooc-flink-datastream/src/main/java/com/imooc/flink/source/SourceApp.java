package com.imooc.flink.source;

import com.imooc.flink.transformation.Access;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class SourceApp {
    public static void main(String[] args) throws Exception {
        // 上下文，是一个框，什么都可以装, 可以理解为环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(2);

//        test05(env);
//        test03(env);
//        test04(env);
        test06(env);
//        StreamExecutionEnvironment.createLocalEnvironment();
//        StreamExecutionEnvironment.createLocalEnvironment(3);
//        StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        StreamExecutionEnvironment.createRemoteEnvironment();

//        DataStreamSource<Long> source = env.fromParallelCollection(new NumberSequenceIterator(1, 10), Long.class);
//        System.out.println("parallel source..." + source.getParallelism());
//
//        source.filter(new FilterFunction<Long>() {
//            @Override
//            public boolean filter(Long value) throws Exception {
//                return value >=5;
//            }
//        }).print();

//        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);
//        System.out.println("source..." + source.getParallelism());
//
//        // 接收socket过来的数据，一行一个单词，过滤PK
//        SingleOutputStreamOperator<String> filterStream = source.filter(new FilterFunction<String>() {
//            @Override
//            public boolean filter(String s) throws Exception {
//                return !"pk".equals(s);
//            }
//        });
//        System.out.println("filter..." + filterStream.getParallelism());
//        filterStream.print();

        env.execute("SourceApp");
    }

    public static void test03(StreamExecutionEnvironment env) {
//        DataStreamSource<Access> source = env.addSource(new AccessSource()).setParallelism(2);
        DataStreamSource<Access> source = env.addSource(new AccessSourceV2()).setParallelism(3);
        System.out.println(source.getParallelism());
        source.print();
    }
    public static void test04(StreamExecutionEnvironment env) {
        DataStreamSource<Student> source = env.addSource(new StudentSource()).setParallelism(4);
        System.out.println(source.getParallelism());
        source.print();
    }

    public static void test05(StreamExecutionEnvironment env) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("flink-topic", new SimpleStringSchema(), properties).setStartFromEarliest());

        System.out.println("kafka ... " + env.getParallelism());
        stream.print();
    }

    public static void test06(StreamExecutionEnvironment env) {
        DataStreamSource<Student> source = env.addSource(new StudentSourceV2()).setParallelism(3);
        System.out.println(source.getParallelism());
        source.print();
    }
}
