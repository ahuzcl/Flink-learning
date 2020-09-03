package com.cunliang.op;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class hotGoodTopN {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<MyBehavior> process = streamSource.process(new ProcessFunction<String, MyBehavior>() {


            @Override
            public void processElement(String input, Context context, Collector<MyBehavior> collector) throws Exception {

                MyBehavior myBehavior = JSON.parseObject(input, MyBehavior.class);
                collector.collect(myBehavior);

            }
        });


        SingleOutputStreamOperator<MyBehavior> myBehaviorSingleOutputStreamOperator = process.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<MyBehavior>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(MyBehavior myBehavior) {
                return myBehavior.timestamp;
            }
        });


        KeyedStream<MyBehavior, Tuple> myBehaviorTupleKeyedStream = myBehaviorSingleOutputStreamOperator.keyBy("itemId", "type");

        WindowedStream<MyBehavior, Tuple, TimeWindow> window = myBehaviorTupleKeyedStream.window(SlidingEventTimeWindows.of(Time.minutes(10),Time.minutes(1)));


        SingleOutputStreamOperator<ItemViewCount> result = window.apply(new WindowFunction<MyBehavior, ItemViewCount,
                        Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple tuple, TimeWindow window, Iterable<MyBehavior> input,
                              Collector<ItemViewCount> out) throws Exception {
                //拿出分组的字段
                String itemId = tuple.getField(0);
                String type = tuple.getField(1);

                //拿出窗口的起始和结束时间
                long start = window.getStart();
                long end = window.getEnd();

                // 编写累加的逻辑
                int count = 0;

                for (MyBehavior myBehavior : input) {
                    count += 1;
                }

                //输出结果
                out.collect(ItemViewCount.of(itemId, type, start, end, count));
            }
        });

        result.print();
        env.execute("HotGoodsTopN");

    }
}

