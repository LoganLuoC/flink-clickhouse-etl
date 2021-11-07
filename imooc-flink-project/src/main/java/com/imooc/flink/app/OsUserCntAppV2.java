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
import scala.Int;

/**
 * 按照新老用户分析
 */
public class OsUserCntAppV2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Access> cleanStream = env.readTextFile("data/access.json")
                .map(new MapFunction<String, Access>() {
                    @Override
                    public Access map(String value) throws Exception {
                        try { // 捕获异常
                            return JSON.parseObject(value, Access.class);
                        } catch (Exception e) {  // 出现异常，则抓取异常值
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

        // TODO... 新老用户 ==> wc
        cleanStream.map(new MapFunction<Access, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Access value) throws Exception {
                return Tuple2.of(value.nu, 1);
            }
        }).keyBy(x -> x.f0).sum(1).print();

        /**
         * (iOS,0,24)
         * (iOS,1,43)
         * (Android,1,24)
         * (Android,0,9)
         */

        /**
         * (0,33)
         * (1,67)
         */

        env.execute("OsUserCntAppV1");
    }
}
