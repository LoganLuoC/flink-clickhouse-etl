package com.imooc.flink.app;

import com.alibaba.fastjson.JSON;
import com.imooc.flink.domian.Access;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


/**
 * 按照device 进行进行判断是否是新老用户
 * 思考 采用的方式和逻辑
 * 1. device放到state？
 * 2. 实现方式： state + 布隆过滤器, 此方式的性能最好
 */
public class OsUserCntAppV3 {
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
                .filter(new FilterFunction<Access>() {  // 启动日志，过滤得到新老用户
                    @Override
                    public boolean filter(Access value) throws Exception {
                        return "startup".equals(value.event);
                    }
                });

        cleanStream.keyBy(x -> x.deviceType)
                .process(new KeyedProcessFunction<String, Access, Access>() {

                    private transient ValueState<BloomFilter<String>> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<BloomFilter<String>> descriptor = new ValueStateDescriptor("s", TypeInformation.of(new TypeHint<BloomFilter<String>>() {
                        }));
                        state = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(Access access, Context context, Collector<Access> collector) throws Exception {

                        String device = access.device;
                        BloomFilter<String> bloomFilter = state.value();
                        if (bloomFilter == null) {  // 实现bloomFilter
                            bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 100000);
                        }
                        if (!bloomFilter.mightContain(device)) {
                            access.nu = 1;
                            bloomFilter.put(device);
                            state.update(bloomFilter);
                        }
                        collector.collect(access);
                    }
                }).print();

        env.execute("OsUserCntAppV1");
    }

}
