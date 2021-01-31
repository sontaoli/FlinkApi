package com.list.flink.transformation;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import org.apache.flink.core.fs.Path;
import org.springframework.util.StringUtils;

import java.util.*;

/**
 * 现有一个配置文件存储车牌号与车主的真实姓名，
 * 通过数据流中的车牌号实时匹配出对应的车主姓名
 * （注意：配置文件可能实时改变）
 * 配置文件可能实时改变  读取配置文件的适合  readFile
 * stream1.connect(stream2)
 */
public class CoFlatMap {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        String filePath="D:\\spring-demo\\FlinkApi\\data\\carId2Name";
        //10s 加载一次
        DataStreamSource<String> carId2NameStream = env.readFile(new TextInputFormat(new Path(filePath)), filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 10000);
        DataStreamSource<String> dataStream = env.socketTextStream("192.168.201.128", 8888);

        dataStream.print();
        dataStream.connect(carId2NameStream).map(new CoMapFunction<String, String, String>() {
            HashMap<String, String> map = new HashMap<String, String>();Map<String, String> hashMap = Collections.synchronizedMap(map);
//            volatile Map<String, String> hashMap = new HashMap<String, String>();
            @Override
            public String map1(String value) throws Exception {
                System.out.println("hashMap:"+hashMap.toString());
                  value = hashMap.getOrDefault(value, "not found name");
                System.out.println("map1:"+value);
                return   value;
            }

            @Override
            public String map2(String value) throws Exception {
                List<String> splits=Arrays.asList(value.split(" "));
                hashMap.put(splits.get(0),splits.get(1));
                System.out.println("map2 hashMap:"+hashMap.toString());
                return value + "加载完毕...";
            }
        }).print();

        env.execute("qqq");
    }

}
