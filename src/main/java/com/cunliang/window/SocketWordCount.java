package com.cunliang.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SocketWordCount {

    public static void main(String[] args) throws Exception {

        String host = "localhost";
        Integer port = 8888;
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = executionEnvironment.socketTextStream(host, port);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {


            public void flatMap(String lines, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = lines.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);
                    collector.collect(tuple2);
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> summ = wordAndOne.keyBy(0).sum(1);
        summ.print();
        executionEnvironment.execute("SocketWordCount");
    }
}
