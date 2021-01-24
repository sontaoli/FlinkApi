package com.list.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

class SourceFunctionMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //source的并行度为1 单并行度source源
        DataStreamSource dataStreamSource = env.addSource((new SourceFunction<String>( ) {
            boolean flag = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                Random random = new Random();
                while (flag) {
                    ctx.collect("list " + random.nextInt(1000));
                    Thread.sleep(200);
                }
            }

            //停止产生数据
            @Override
            public void cancel() {
                flag = false;

            }
        }));
        dataStreamSource.print();
        env.execute("API  SourceFunctionMain");
    }
}
