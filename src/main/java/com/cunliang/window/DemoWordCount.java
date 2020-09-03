package com.cunliang.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class DemoWordCount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> source = environment.socketTextStream("localhost", 8888);
        int parallelism = source.getParallelism();

        System.out.println(parallelism);


        source.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {

                String[] s1 = s.split(" ");

                return Tuple2.of(s1[0],1);
            }
        }).keyBy(0)
               // .timeWindow(Time.seconds(5),Time.seconds(10))
                .sum(1)
                .print();




        environment.execute();

    }
}
