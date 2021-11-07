package com.imooc.flink.udf;

import com.imooc.flink.domian.ProductEventNameTopN;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class TopNValueStateKPF extends KeyedProcessFunction<Tuple4<String, String, Long, Long>, ProductEventNameTopN, List<ProductEventNameTopN>> {

    private transient ValueState<List<ProductEventNameTopN>> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 需要list状态来容错，使用List描述符
//                listState = getRuntimeContext().getListState(new ListStateDescriptor<ProductEventNameTopN>("list-cnt", ProductEventNameTopN.class));
        ValueStateDescriptor<List<ProductEventNameTopN>> descriptor = new ValueStateDescriptor<>("cnt-state", TypeInformation.of(new TypeHint<List<ProductEventNameTopN>>() {
        }));
        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(ProductEventNameTopN value, Context context, Collector<List<ProductEventNameTopN>> collector) throws Exception {
        List<ProductEventNameTopN> lists = state.value();
        if (lists == null) {  // 初始化lists
            lists = new ArrayList<ProductEventNameTopN>();
        }

        lists.add(value);
        state.update(lists);  // 值状态需手动更新

        // 什么时候出发窗口执行，注册一个定时器,窗口结束时间 + 1， 还可以多注册定时器
        context.timerService().registerEventTimeTimer(value.end + 1);
    }

    // 满足条件，触发定时器，完成TopN操作
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<List<ProductEventNameTopN>> out) throws Exception {
        List<ProductEventNameTopN> list = state.value();
        list.sort((x, y) -> Long.compare(y.count, x.count));  // lambda 表达式， 降序排列总数

        ArrayList<ProductEventNameTopN> sorted = new ArrayList<>();
        for (int i = 0; i < Math.min(3, list.size()); i++) {  // 注意事件类别没有3个的情况
            sorted.add(list.get(i));
        }

        out.collect(sorted);
    }
}
