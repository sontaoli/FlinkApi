package com.list.flink.transformation;

import com.list.flink.pojo.WordWithCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FlowAnalyCar02 {
    /**
     * demo02：从kafka中消费数据，统计每一个分钟每一个卡口的流量   构建组合key
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.48.131:9092");
        props.setProperty("group.id", "flink-kafka-04");
        FlinkKafkaConsumer flinkKafkaConsumer = new FlinkKafkaConsumer("flink-kafka", new SimpleStringSchema(), props);
        flinkKafkaConsumer.setStartFromEarliest();
        //测试用数据源
        DataStreamSource<String> stringDataStreamSource = env.addSource(flinkKafkaConsumer).setParallelism(1);
        stringDataStreamSource.print().setParallelism(1);
        SingleOutputStreamOperator<WordWithCount> words = stringDataStreamSource.flatMap(new FlatMapFunction<String, WordWithCount>() {
            //stream 中元素类型 变成二元组类型  kv stream   k:monitor_id v:1
            @Override
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                List<String> strings = Arrays.asList(value.split("\t"));
//                2014-08-20 14:10
                String time = strings.get(2).substring(0, strings.get(2).lastIndexOf(":"));
                out.collect(new WordWithCount(strings.get(0)+time, 1L));
            }
        }).keyBy("word").reduce(new ReduceFunction<WordWithCount>() {
            @Override
            public WordWithCount reduce(WordWithCount value1, WordWithCount value2) throws Exception {
                return new WordWithCount(value1.word, value1.count + value2.count);
            }
        });
        words.print();
        env.execute();

    }
}
