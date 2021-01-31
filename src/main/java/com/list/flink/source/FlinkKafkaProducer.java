package com.list.flink.source;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FlinkKafkaProducer {
    public static void main(String[] args) throws InterruptedException {
        //配置连接kafka的信息
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.201.128:9092");
        properties.put("buffer.memory", 33554432);
        properties.put("retries", 0);
        properties.put("acks", "all");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //创建一个kafka 生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        List<String> files = readFile("D:\\spring-demo\\FlinkApi\\data\\carFlow_all_column_test.txt");
        for (int i = 0; i < files.size(); i++) {
            String elem = files.get(i);
            List<String> splits = Arrays.asList(elem.split(","));
            String monitorId = splits.get(0).replace("'", "");
            String carId = splits.get(2).replace("'", "");
            String timestamp = splits.get(4).replace("'", "");
            String speed = splits.get(6);
            StringBuilder builder = new StringBuilder();
            StringBuilder info = builder.append(monitorId + "\t").append(carId + "\t").append(timestamp + "\t").append(speed);
            producer.send(new ProducerRecord<String, String>("flink-kafka", i + "", info.toString()));
            Thread.sleep(200);
        }
    }

    private static List<String> readFile(String path) {
        List<String> files = new ArrayList<>();
        File file = new File(path);
        BufferedReader reader = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));

            int line = 1;
            String temp;
            // 一次读入一行，直到读入null为文件结束
            while ((temp = reader.readLine()) != null) {
                files.add(temp);
                if (line > 100) {
                    return files;
                }
                line++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        return files;
    }
}
