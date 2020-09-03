package com.cunliang.op;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class AggregateFunctionMain {


    public static void main(String[] args) {



        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //senv.socketTextStream("localhost",8888);
        DataStream<String> sourceData = senv.readTextFile("./src/lib/a.txt");

        DataStream<ProductViewData> productViewData = sourceData.map(new MapFunction<String, ProductViewData>() {
            @Override
            public ProductViewData map(String value) throws Exception {
                //return null;
                String[] record = value.split(" ");
                return new ProductViewData(record[0], record[1], Long.valueOf(record[2]),
                        Long.valueOf(record[3]));
            }
        });


    }



}
