package com.imooc.flink.transformation;

import com.imooc.flink.source.AccessSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

public class TranformationApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        map(env);
//        flatMap(env);
//        keyBy(env);
//        reduce(env);
//        richMap(env);
//        union(env);
//        connect(env);
//        coMap(env);
        coFlatMap(env);
        env.execute();
    }

    /**
     *  数据是一行行的
     * @param env
     * TOTO... 每行数据 ==> Access
     */
    public static void map(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.readTextFile("data/access.log");

        SingleOutputStreamOperator<Access> mapstream = source.map(new MapFunction<String, Access>() {
            @Override
            public Access map(String s) throws Exception {
                String[] splits = s.split(",");
                long time = Long.parseLong(splits[0].trim());
                String domain = splits[1].trim();
                Double traffic = Double.parseDouble(splits[2].trim());
                return new Access(time, domain, traffic);
            }
        });

        SingleOutputStreamOperator<Access> filterstream = mapstream.filter(new FilterFunction<Access>() {
            @Override
            public boolean filter(Access access) throws Exception {
                return access.getTraffic() > 3000;
            }
        });
        filterstream.print();
    }

    /**
     * CoMapFunction, 对每个输入的流进行单独处理
     * @param env
     */
    public static void coFlatMap(StreamExecutionEnvironment env) {
        DataStreamSource<String> source1 = env.fromElements("a b c", "aa bb cc");
        DataStreamSource<String> source2 = env.fromElements("1,2,3", "11,22,33");

        source1
                .connect(source2)
                .flatMap(new CoFlatMapFunction<String, String, String>() {
            @Override
            public void flatMap1(String value, Collector<String> out) throws Exception {
                String[] splits = value.split(" ");
                for (String split:
                splits){
                    out.collect(split);
                }
            }

            @Override
            public void flatMap2(String value, Collector<String> out) throws Exception {
                String[] splits = value.split(",");
                for (String split :
                        splits) {
                    out.collect(split);
                }
            }
        }).print();
    }

    public static void coMap(StreamExecutionEnvironment env) {
        DataStreamSource<Access> stream1 = env.addSource(new AccessSource());
        SingleOutputStreamOperator<String> stream2 = env.addSource(new AccessSource()).map(
                new MapFunction<Access, String>() {
                    @Override
                    public String map(Access value) throws Exception {
                        return value.toString();
                    }
                }
        );

        stream1.connect(stream2).map(new CoMapFunction<Access, String, String>() {
            @Override
            public String map1(Access value) throws Exception {
                return value.toString();
            }

            @Override
            public String map2(String value) throws Exception {
                return "String ===> " + value;
            }
        }).print();

    }

    public static void union(StreamExecutionEnvironment env) {
        DataStreamSource<String> source1 = env.socketTextStream("localhost", 9527);
        DataStreamSource<String> source2 = env.socketTextStream("localhost", 9528);

//        source1.union(source2).print();
        source1.union(source1).print();

    }

    /**
     * union 多流合并，需要相同类型的数据结构
     * connect 2流合并， 数据类型可以不同, 可以对不同的流进行处理
     * @param env
     */
    public static void connect(StreamExecutionEnvironment env) {
        DataStreamSource<Access> source1 = env.addSource(new AccessSource());
        DataStreamSource<Access> source2 = env.addSource(new AccessSource());

        SingleOutputStreamOperator<Tuple2<String, Access>> stream2 = source1.map(new MapFunction<Access, Tuple2<String, Access>>() {
            @Override
            public Tuple2<String, Access> map(Access value) throws Exception {
                return Tuple2.of("test", value);
            }
        });

        source1.connect(stream2).map(new CoMapFunction<Access, Tuple2<String, Access>, String>() {
            @Override
            public String map1(Access value) throws Exception {
                return value.toString();
            }

            @Override
            public String map2(Tuple2<String, Access> value) throws Exception {
                return value.f0 + "===>" + value.f1.toString();
            }
        }).print();
//        ConnectedStreams<Access, Access> connect = source1.connect(source1);
//
//        connect.map(new CoMapFunction<Access, Access, Access>() {
//            @Override
//            public Access map1(Access value) throws Exception {
//                return value;
//            }
//
//            @Override
//            public Access map2(Access value) throws Exception {
//                return value;
//            }
//        }).print();

    }

    public static void richMap(StreamExecutionEnvironment env) {
        env.setParallelism(3);
        DataStreamSource<String> source = env.readTextFile("data/access.log");

        SingleOutputStreamOperator<Access> mapstream = source.map(new PKMapFunction() {

        });
        mapstream.print();

    }

    /**
     * wc: socket
     * 进来的数据： pk，pk
     * WC需求分析：
     * 1. 数据读取
     * 2。 按指定分隔符分割
     * 3。 为每个单词附上值1
     * 4。 按照单词进行keyBy
     * 5。 分组求和
     * @param env
     */
    public static void reduce(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);

        source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] splits = s.split(",");
                for (String word: splits) {
                    collector.collect(word.trim());
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        }).keyBy(x->x.f0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                        return new Tuple2<>(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + t1.f1);
                    }
                }).print();
//                .sum(1).print();
    }
    /**
     * 按照domain分组，求traffic的和
     * @param env
     */
    public static void keyBy(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.readTextFile("data/access.log");

        SingleOutputStreamOperator<Access> mapstream = source.map(new MapFunction<String, Access>() {
            @Override
            public Access map(String s) throws Exception {
                String[] splits = s.split(",");
                long time = Long.parseLong(splits[0].trim());
                String domain = splits[1].trim();
                Double traffic = Double.parseDouble(splits[2].trim());
                return new Access(time, domain, traffic);
            }
        });

//        mapstream.keyBy(new KeySelector<Access, String>() {
//            @Override
//            public String getKey(Access access) throws Exception {
//                return access.getDomain();
//            }
//        }).sum("traffic").print();
        mapstream.keyBy(x -> x.getDomain()).sum("traffic").print();

        SingleOutputStreamOperator<Access> filterstream = mapstream.filter(new FilterFunction<Access>() {
            @Override
            public boolean filter(Access access) throws Exception {
                return access.getTraffic() > 3000;
            }
        });
//        filterstream.print();
    }

    /**
     * 数据按照行进入：flink,spark,pk,pk,flink
     * 需求：
     * 1. 把每行数据按照逗号分割
     * 2. 单词拆开，过滤pk
     * @param env
     */
    public static void flatMap(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("", 9527);
        source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] splits = s.split(",");
                for ( String word: splits) {
                    collector.collect(word);
                }
            }
        }).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return !"pk".equals(s);
            }
        })
                .print();
    }
}
