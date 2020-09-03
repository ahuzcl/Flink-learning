package com.cunliang.KafkaBackup;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import sun.print.PeekGraphics;

import java.util.Properties;

public class KafkaJson {

    public static void main(String[] args) {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.server", "111111");
        properties.setProperty("groupid", "footprint");

        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        FlinkKafkaConsumer010<ObjectNode> footprint = new FlinkKafkaConsumer010<ObjectNode>(
                "footprint",
                new JSONKeyValueDeserializationSchema(true),
                properties

        );
        DataStreamSource<ObjectNode> dataStreamSource = executionEnvironment.addSource(footprint);

        SingleOutputStreamOperator<ObjectNode> process = dataStreamSource.process(new ProcessFunction<ObjectNode, ObjectNode>() {
            @Override
            public void processElement(ObjectNode jsonNodes, Context context, Collector<ObjectNode> collector) throws Exception {

                JsonNode node = jsonNodes.get("user").get("master_id");
                collector.collect((ObjectNode) node);

            }
        });


    }

}
