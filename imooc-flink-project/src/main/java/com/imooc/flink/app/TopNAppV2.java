package com.imooc.flink.app;

import com.alibaba.fastjson.JSON;
import com.imooc.flink.domian.Access;
import com.imooc.flink.domian.ProductEventNameTopN;
import com.imooc.flink.udf.TopNAggregateFunction;
import com.imooc.flink.udf.TopNValueStateKPF;
import com.imooc.flink.udf.TopNWindowFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;


/**
 * 按照操作系统维度进行新老用户分析
 */
public class TopNAppV2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Access> cleanStream = env.readTextFile("data/access.json")
                .map(new MapFunction<String, Access>() {
                    @Override
                    public Access map(String value) throws Exception {
                        // json ==> 自定义对象
                        try {
                            return JSON.parseObject(value, Access.class);
                        } catch (Exception e) {
                            e.printStackTrace();
                            // TODO... 把这些异常的数据记录到某个地方去
                            return null;
                        }
                    }
                }).filter(x -> x != null)
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<Access>(Time.seconds(20)) {
                            @Override
                            public long extractTimestamp(Access element) {
                                return element.time;
                            }
                        }
                ).filter(new FilterFunction<Access>() {
                    @Override
                    public boolean filter(Access value) throws Exception {
                        return !"startup".equals(value.event);
                    }
                });

//        cleanStream.print();

        // 5分钟内不同event类型，类别，商品TopN的访问量
        // process 全量数据再执行
        // reduce 增量数据执行
        // aggregete 先增量统计 再全量执行
        SingleOutputStreamOperator<ProductEventNameTopN> aggStream = cleanStream.keyBy(new KeySelector<Access, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> getKey(Access value) throws Exception {
                return Tuple3.of(value.event, value.product.category, value.product.name);
            }
        }).window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
                .aggregate(new TopNAggregateFunction(), new TopNWindowFunction());

//        aggStream.print();

        SingleOutputStreamOperator<List<ProductEventNameTopN>> result = aggStream.keyBy(new KeySelector<ProductEventNameTopN, Tuple4<String, String, Long, Long>>() {
            @Override
            public Tuple4<String, String, Long, Long> getKey(ProductEventNameTopN value) throws Exception {
                return Tuple4.of(value.event, value.catagory, value.start, value.end);  // 已经计算过维度name， 用其他4个维度统计
            }
        }).process(new TopNValueStateKPF());

        // 如何写到数据库
        result.print();

        env.execute("TopNAppV1");
    }
}
