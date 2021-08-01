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
import scala.Int;

/**
 * 按照操作系统进行新老用户的分析
 */
public class OsUserCntAppV1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Access> cleanStream = env.readTextFile("data/access.json")
                .map(new MapFunction<String, Access>() {
                    @Override
                    public Access map(String value) throws Exception {
                        try {
                            // TODO json ==> Access
                            return JSON.parseObject(value, Access.class);
                        } catch (Exception e) {
                            return null;
                        }
                    }
                }).filter(x -> x != null)
                .filter(x -> "startup".equals(x.event));

        /**
         * (iOS,1,43)
         * (Android,1,24)
         * (iOS,0,24)
         * (Android,0,9)
         *
         */
        // TODO... 操作系统 新老用户 ==> wc
        cleanStream.map(new MapFunction<Access, Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> map(Access value) throws Exception {
                return Tuple3.of(value.os, value.nu, 1);
            }
        }).keyBy(new KeySelector<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> getKey(Tuple3<String, Integer, Integer> value) throws Exception {
                return Tuple2.of(value.f0, value.f1);
            }
        }).sum(2).print().setParallelism(1);

        env.execute("OsUserCntAppV1");
    }
}
