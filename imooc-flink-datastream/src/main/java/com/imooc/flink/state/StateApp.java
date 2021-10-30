package com.imooc.flink.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class StateApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();

        test01(env);

        env.execute("WindowApp");
    }

    /**
     * 使用ValueState实现平均数
     *
     * @param env
     */
    public static void test01(StreamExecutionEnvironment env) {

        ArrayList<Tuple2<Long, Long>> list = new ArrayList<>();

        list.add(Tuple2.of(1L, 4L));
        list.add(Tuple2.of(1L, 5L));
        list.add(Tuple2.of(2L, 8L));
        list.add(Tuple2.of(2L, 4L));
        list.add(Tuple2.of(3L, 4L));
        list.add(Tuple2.of(3L, 6L));

        env.fromCollection(list)
                .keyBy(x -> x.f0)
                .flatMap(new AvgWithValueState())
                .print();
    }
}

class AvgWithValueState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

    // 求平均数： 记录条数，求和
    private transient ValueState<Tuple2<Long, Long>> sum;

    // 在open时，获得描述器对象
    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<>(
                "avg",
                Types.TUPLE(Types.LONG, Types.LONG)
        );
        sum = getRuntimeContext().getState(descriptor);
    }

    // 实现flatMap的算子操作
    @Override
    public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Double>> out) throws Exception {
        // TODO.. ==> state 次数和总和

        Tuple2<Long, Long> currentState = sum.value();

        if (null == currentState) currentState = Tuple2.of(0L, 0L);

        currentState.f0 += 1;  // 次数
        currentState.f1 += value.f1;  // 求和

        sum.update(currentState);

        // 达到3条数据 ==> 求平均数  clear
        if (currentState.f0 >= 2) {
            out.collect(Tuple2.of(value.f0, currentState.f1/currentState.f0.doubleValue()));
            sum.clear();
        }
    }
}