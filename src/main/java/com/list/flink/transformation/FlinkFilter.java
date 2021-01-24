package com.list.flink.transformation;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

class FlinkFilter {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkFilter.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> numbers = env.fromCollection( Arrays.asList(1, 2, 5));
        //过滤掉偶数，保留计算
        DataStream<Integer> result = numbers.filter(i -> i % 2 != 0).setParallelism(2);
        result.print().setParallelism(1);
        env.execute("API Skeleton Filter");
    }
}
