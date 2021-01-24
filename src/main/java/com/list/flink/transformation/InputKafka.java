package com.list.flink.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

class InputKafka {
    public static void main(String[] args) throws Exception {
        System.out.println("---------");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        //注意   sparkstreaming + kafka（0.10之前版本） receiver模式  zookeeper url（元数据）
        props.setProperty("bootstrap.servers","192.168.48.131:9092");
        props.setProperty("group.id","flink-kafka-01");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        //测试用数据源
        DataStreamSource<String> stringDataStreamSource =env.addSource(new FlinkKafkaConsumer("topic01", new SimpleStringSchema(),
                props));
        stringDataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                System.out.println("value= "+value + Thread.currentThread().getName());
                return value;
            }
        });
        stringDataStreamSource.print().setParallelism(1);
        env.execute("InputKafka");
    }
}
