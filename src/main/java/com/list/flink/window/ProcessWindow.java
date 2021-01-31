package com.list.flink.window;

import com.list.flink.pojo.DspProcessPojo;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 2020-01-31 17:07:10,123,44567,4577
 * 2020-01-31 17:07:10,123,44567,4577
 * eventTime,triceId,sendTime,receveTime
 */
public class ProcessWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        String filePath="D:\\spring-demo\\FlinkApi\\data\\carId2Name";
        //10s 加载一次
        DataStreamSource<String> carId2NameStream = env.readFile(new TextInputFormat( new Path(filePath)), filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 10000);
        DataStreamSource<String> dataStream = env.socketTextStream("192.168.201.128", 8888);

        SingleOutputStreamOperator<DspProcessPojo> mapStream = dataStream.map(new MapFunction<String, DspProcessPojo>() {
            @Override
            public DspProcessPojo map(String value) throws Exception {
                String[] split = value.split(",");
                DspProcessPojo dspProcessPojo = new DspProcessPojo();
                if(split.length>2){
                    dspProcessPojo.setLogTime(split[0]);
                    dspProcessPojo.setTraceId(split[1]);
                    dspProcessPojo.setSendTime(Integer.parseInt(split[2]));
                    dspProcessPojo.setReceveTime(Integer.parseInt(split[3]));
                }else {
                    dspProcessPojo.setLogTime("1");
                    dspProcessPojo.setTraceId("1");
                    dspProcessPojo.setSendTime(0);
                    dspProcessPojo.setReceveTime(0);
                }

                return dspProcessPojo;
            }
        });
        mapStream.timeWindowAll(Time.seconds(10)).process(new MyProcessWindowFunction()).print();
        env.execute("qqq");
    }

}
 class MyProcessWindowFunction extends ProcessAllWindowFunction<DspProcessPojo, List<DspProcessPojo>, TimeWindow> {

     @Override
     public void process(Context context, Iterable<DspProcessPojo> elements, Collector<List<DspProcessPojo>> out) throws Exception {
         List<DspProcessPojo> list = new ArrayList<>();
         elements.forEach(o->{ list.add(o); });
         Map<String, List<DspProcessPojo>> collect = list.stream().collect(Collectors.groupingBy(DspProcessPojo::getTraceId));
         list.clear();
         collect.forEach((k,v)->{
             Optional<DspProcessPojo> min = v.stream().min(Comparator.comparingInt(DspProcessPojo::getSendTime));
             Optional<DspProcessPojo> max = v.stream().max(Comparator.comparingInt(DspProcessPojo::getReceveTime));
              int  elapsedTime = max.get().getReceveTime()-min.get().getSendTime();
             System.out.println("traceId== "+max.get().getTraceId()+" 耗时==> "+elapsedTime);
             v.forEach(o-> o.setElapsedTime(elapsedTime));
             list.addAll(v);
         });
         out.collect(list);

     }
 }