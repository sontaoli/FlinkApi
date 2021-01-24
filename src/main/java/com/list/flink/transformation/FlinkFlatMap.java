package com.list.flink.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FlinkFlatMap {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkFlatMap.class);
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String[] words = {"apple","orange","banana","watermelon"};
        //创建DataSource
        //根据给定的元素创建一个数据流，元素必须是相同的类型，比如全部为String，或者全部为int。
        DataStreamSource<String> ds = env.fromElements(words);
        //Transformations
        //对DataStream应用一个flatMap转换。对DataStream中的每一个元素都会调用FlatMapFunction接口的具体实现类。
        // flatMap方法可以返回任意个元素，当然也可以什么都不返回。
        SingleOutputStreamOperator<String> flatMap = ds.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                out.collect(value);
                out.collect(value.toUpperCase());
            }
        });
        flatMap.print().setParallelism(1);
        //sinks打印出信息
        //给DataStream添加一个Sinks
//        flatMap.addSink(new SinkFunction<String>() {
//            @Override
//            public void invoke(String value) throws Exception {
//                LOG.info(value);
//            }
//        });

        env.execute("API Skeleton FlatMap");
    }
}
