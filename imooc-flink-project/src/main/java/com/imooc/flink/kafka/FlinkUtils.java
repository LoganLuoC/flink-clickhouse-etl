package com.imooc.flink.kafka;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class FlinkUtils {
    public static StreamExecutionEnvironment env;

    public static <T> DataStreamSource<T> KafkaConsumerV3(String[] args, Class<? extends KafkaDeserializationSchema<T>> deserializer) throws Exception {
        ParameterTool tool = ParameterTool.fromPropertiesFile(args[0]);
        String groupId = tool.get("group.id", "test001");
        String servers = tool.getRequired("bootstrap.servers");
        List<String> topics = Arrays.asList(tool.getRequired("kafka.input.topics").split(","));
        String autoCommit = tool.get("enable.auto.commit", "false");
        String autoOffsetReset = tool.get("auto.offset.reset", "earliest");

        int interval = tool.getInt("checkpoint.Interval", 5000);
        String ckPath = tool.get("checkpoint.fs", "file:///Users/carves/Documents/projects/flink-learn/state");

        // Kafka 配置参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", servers);
        properties.setProperty("group.id", groupId);
        properties.setProperty("enable.auto.commit", autoCommit);
        properties.setProperty("auto.offset.reset", autoOffsetReset);

        env = StreamExecutionEnvironment.getExecutionEnvironment();

        // checkpoint 参数，可以在配置文件中修改
        env.enableCheckpointing(interval);
        env.setStateBackend(new FsStateBackend(ckPath));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.of(5, TimeUnit.SECONDS)));

        // consumer 配置

        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<>(topics, deserializer.newInstance(), properties);

        return env.addSource(kafkaConsumer);
    }

    public static <T> DataStreamSource<T> KafkaConsumerV2(String[] args, Class<? extends DeserializationSchema<T>> deserializer) throws Exception {
        ParameterTool tool = ParameterTool.fromPropertiesFile(args[0]);
        String groupId = tool.get("group.id", "test001");
        String servers = tool.getRequired("bootstrap.servers");
        List<String> topics = Arrays.asList(tool.getRequired("kafka.input.topics").split(","));
        String autoCommit = tool.get("enable.auto.commit", "false");
        String autoOffsetReset = tool.get("auto.offset.reset", "earliest");

        int interval = tool.getInt("checkpoint.Interval", 5000);
        String ckPath = tool.get("checkpoint.fs", "file:///Users/carves/Documents/projects/flink-learn/state");

        // Kafka 配置参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", servers);
        properties.setProperty("group.id", groupId);
        properties.setProperty("enable.auto.commit", autoCommit);
        properties.setProperty("auto.offset.reset", autoOffsetReset);

        env = StreamExecutionEnvironment.getExecutionEnvironment();

        // checkpoint 参数，可以在配置文件中修改
        env.enableCheckpointing(interval);
        env.setStateBackend(new FsStateBackend(ckPath));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.of(5, TimeUnit.SECONDS)));

        // consumer 配置

        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<>(topics, deserializer.newInstance(), properties);

        return env.addSource(kafkaConsumer);
    }

    public static  DataStreamSource<String> KafkaConsumerV1(ParameterTool tool) throws IOException {
        String groupId = tool.get("group.id", "test001");
        String servers = tool.getRequired("bootstrap.servers");
        List<String> topics = Arrays.asList(tool.getRequired("kafka.input.topics").split(","));
        String autoCommit = tool.get("enable.auto.commit", "false");
        String autoOffsetReset = tool.get("auto.offset.reset", "earliest");

        int interval = tool.getInt("checkpoint.Interval", 5000);
        String ckPath = tool.get("checkpoint.fs", "file:///Users/carves/Documents/projects/flink-learn/state");

        // Kafka 配置参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", servers);
        properties.setProperty("group.id", groupId);
        properties.setProperty("enable.auto.commit", autoCommit);
        properties.setProperty("auto.offset.reset", autoOffsetReset);

        env = StreamExecutionEnvironment.getExecutionEnvironment();

        // checkpoint 参数，可以在配置文件中修改
        env.enableCheckpointing(interval);
        env.setStateBackend(new FsStateBackend(ckPath));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.of(5, TimeUnit.SECONDS)));

        // consumer 配置

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topics, new SimpleStringSchema(), properties);

        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        return source;
    }
    public static void main(String[] args) throws IOException {
        ParameterTool tool = ParameterTool.fromPropertiesFile(args[0]);
        // 参数分为2类 1 必填 2 选填
        // 配置文件放置在外面 可以直接修改
        String groupId = tool.get("group.id", "test001");
        String servers = tool.getRequired("bootstrap.servers");

        System.out.println(groupId);
        System.out.println(servers);

    }
}
