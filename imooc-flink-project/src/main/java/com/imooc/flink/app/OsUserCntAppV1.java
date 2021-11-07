package com.imooc.flink.app;

import com.alibaba.fastjson.JSON;
import com.imooc.flink.domian.Access;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;


/**
 * 按照操作系统维度进行新老用户分析
 */
public class OsUserCntAppV1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Access> cleanStream = env.readTextFile("data/access.json")
                .map(new MapFunction<String, Access>() {
                    @Override
                    public Access map(String value) throws Exception {
                        try { // 注意事项：捕获异常，考虑解析的容错性
                            return JSON.parseObject(value, Access.class);
                        } catch (Exception e) {  // 出现异常，则抓取异常值，可以写入到ES
                            e.printStackTrace();
                            return null;
                        }
                    }
                }).filter(x -> x != null)
                .filter(new FilterFunction<Access>() {  // 过滤得到新老用户
                    @Override
                    public boolean filter(Access value) throws Exception {
                        return "startup".equals(value.event);
                    }
                });

        // TODO... 操作系统维度 新老用户 ==> wc
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> result = cleanStream.map(new MapFunction<Access, Tuple3<String, Integer, Integer>>() { // 每条对象数据，生成单条基于os和nu统计数据
            @Override
            public Tuple3<String, Integer, Integer> map(Access value) throws Exception {
                return Tuple3.of(value.os, value.nu, 1);
            }
        }).keyBy(new KeySelector<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>>() {  // 基于单条数据进行keyBy操作,会进行partition分区
            @Override
            public Tuple2<String, Integer> getKey(Tuple3<String, Integer, Integer> value) throws Exception {
                return Tuple2.of(value.f0, value.f1);
            }
        }).sum(2);
        /**
         * (iOS,0,24)
         * (iOS,1,43)
         * (Android,1,24)
         * (Android,0,9)
         */
        result.print();

        // 可以多考虑异步的请求方式

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();

        result.addSink(new RedisSink<Tuple3<String, Integer, Integer>>(conf, new RedisExampleMapper()));

        env.execute("OsUserCntAppV1");
    }
    public static class RedisExampleMapper implements RedisMapper<Tuple3<String, Integer, Integer>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "os-user-cnt:2021-12-01");
        }

        @Override
        public String getKeyFromData(Tuple3<String, Integer, Integer> stringIntegerIntegerTuple3) {
            return stringIntegerIntegerTuple3.f0 + "_" + stringIntegerIntegerTuple3.f1;
        }

        @Override
        public String getValueFromData(Tuple3<String, Integer, Integer> stringIntegerIntegerTuple3) {
            return "" + stringIntegerIntegerTuple3.f2;
        }
    }
}
