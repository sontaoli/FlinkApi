package com.list.flink.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * 输入一个值（10） 当值小于1的时候才被输出
 */
public class FlinkIterate {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Integer> integers = Arrays.asList(10);
        DataStreamSource<Integer> numbers = env.fromCollection(integers).setParallelism(1);
        IterativeStream<Integer> initialStream = numbers.iterate() ;

        IterativeStream<Integer> iteration = initialStream.iterate();
        DataStream<Integer> iterationBody = iteration.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                System.out.println("打印：" + value);
                if (value > 0) {
                    value = value - 1;
                }
                return value;
            }
        }
        ).setParallelism(1);
        DataStream<Integer> feedback = iterationBody.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value > 0;
            }
        }).setParallelism(1);
        iteration.closeWith(feedback);
        DataStream<Integer> output = iterationBody.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value <= 0;
            }
        }).setParallelism(1);
        output.print().setParallelism(1);
        env.execute("FlinkIterate") ;
    }

}
