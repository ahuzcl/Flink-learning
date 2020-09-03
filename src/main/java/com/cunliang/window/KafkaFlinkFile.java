package com.cunliang.window;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class KafkaFlinkFile {
    public static void main(String[] args) {



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //  配置Kafka环境

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-group");
        String inputTopic = "Shakespeare";
        String outputTopic = "WordCount";


    }

}
