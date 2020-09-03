package com.cunliang.kafka;

import javafx.beans.property.SimpleStringProperty;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.util.Properties;

public class KafkaSource {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();

        //指定Kafka broker地址
        properties.setProperty("bootstrap.servers","10.239.13.20:9092");
        //指定组ID
       // properties.setProperty("group.id",args[0]);
        //
      //  properties.setProperty("auto.offset.reset","earliest");


        FlinkKafkaConsumerBase<String> kafkaSource= new FlinkKafkaConsumer<String>("footprint", new SimpleStringSchema(),properties);

        DataStream<String> lines = environment.addSource(kafkaSource);

        lines.print();

        environment.execute("KafkaSource");
    }


}
