package com.cunliang.window;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;

public class SourceDemo {


    public static void main(String[] args) {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source = executionEnvironment.fromParallelCollection(new NumberSequenceIterator(1, 10), Long.class);
        int parallelism = source.getParallelism();
        System.out.println(parallelism);

    }

}
