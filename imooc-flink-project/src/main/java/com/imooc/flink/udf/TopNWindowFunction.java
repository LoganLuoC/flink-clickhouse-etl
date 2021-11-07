package com.imooc.flink.udf;

import com.imooc.flink.domian.ProductEventNameTopN;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Long: 已经聚合的结果
 * ProductEventNameTopN： 统计全量结果
 * Tuple3： key
 * TimeWindow： 计算的窗口
 */
public class TopNWindowFunction implements WindowFunction<Long, ProductEventNameTopN, Tuple3<String, String, String>, TimeWindow> {
    @Override
    public void apply(Tuple3<String, String, String> value, TimeWindow window, Iterable<Long> input, Collector<ProductEventNameTopN> out) throws Exception {
        String event = value.f0;
        String catagory = value.f1;
        String product = value.f2;
        Long count = input.iterator().next();
        long start = window.getStart();
        long end = window.getEnd();

        out.collect(new ProductEventNameTopN(event, catagory, product,count,start, end));
    }
}

