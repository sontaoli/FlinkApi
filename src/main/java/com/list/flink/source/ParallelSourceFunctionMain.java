package com.list.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

class ParallelSourceFunctionMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //多并行度source源
        DataStreamSource dataStreamSource = env.addSource((new ParallelSourceFunction<String>( ) {
            boolean flag = true;

            @Override
            public void run( SourceFunction.SourceContext<String> ctx) throws Exception {
                Random random = new Random();
                while (flag) {
                    ctx.collect("list " + random.nextInt(1000));
                    Thread.sleep(500);
                }
            }
            //停止产生数据
            @Override
            public void cancel() {
                flag = false;

            }
        })).setParallelism(2);
        dataStreamSource.print();
        env.execute("API  SourceFunctionMain");
    }
}
