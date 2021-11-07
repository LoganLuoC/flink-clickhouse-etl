package com.imooc.flink.app;

import com.alibaba.fastjson.JSON;
import com.imooc.flink.domian.Access;
import com.imooc.flink.utils.ProvinceUtils;
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
 * 按照省份维度进行新老用户分析
 */
public class ProvinceUserCntApp {
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
                }).map(new ProvinceUtils());  // .print()

        // TODO... 省份维度 新老用户 ==> wc
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> result = cleanStream.map(new MapFunction<Access, Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> map(Access value) throws Exception {
                return Tuple3.of(value.province, value.nu, 1);
            }
        }).keyBy(new KeySelector<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> getKey(Tuple3<String, Integer, Integer> value) throws Exception {
                return Tuple2.of(value.f0, value.f1);
            }
        }).sum(2);// .print();

        result.print();

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();

        result.addSink(new RedisSink<Tuple3<String, Integer, Integer>>(conf, new RedisExampleMapper()));

        env.execute("ProvinceUserCntApp");
    }
    public static class RedisExampleMapper implements RedisMapper<Tuple3<String, Integer, Integer>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "province-user-cnt:2021-12-01");
        }

        @Override
        public String getKeyFromData(Tuple3<String, Integer, Integer> stringIntegerIntegerTuple3) {
            return stringIntegerIntegerTuple3.f0 + "-" + stringIntegerIntegerTuple3.f1;
        }

        @Override
        public String getValueFromData(Tuple3<String, Integer, Integer> stringIntegerIntegerTuple3) {
            return "" + stringIntegerIntegerTuple3.f2;
        }
    }
}
