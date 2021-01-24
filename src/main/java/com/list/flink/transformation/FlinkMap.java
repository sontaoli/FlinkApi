package com.list.flink.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

class FlinkMap {
    public static void main(String[] args) throws Exception {
        System.out.println("---------");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //测试用数据源
        DataStreamSource<String> stringDataStreamSource1 = env.fromCollection(Arrays.asList("list", "232","set", "232","set", "232","set"));
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = stringDataStreamSource1.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                System.out.println("value= "+value + Thread.currentThread().getName());
                return value;
            }
        }).setParallelism(1);
        System.out.println("-77777-");
        stringSingleOutputStreamOperator.print();
        env.execute("FlinkMain");
    }
}
