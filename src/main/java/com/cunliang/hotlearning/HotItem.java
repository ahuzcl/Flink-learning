package com.cunliang.hotlearning;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class HotItem {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> source = env.readTextFile("./src/main/resources/UserBehavior.csv");
        //source.print();
        SingleOutputStreamOperator<User> flatMap = source.flatMap(new FlatMapFunction<String, User>() {
            public void flatMap(String s, Collector<User> collector) throws Exception {
                String[] split = s.split(",");
                String uid = split[0];
                String itemId = split[1];
                String category = split[2];
                String behavior = split[3];
                Long timestamp = Long.parseLong(split[4]);
                collector.collect(User.of(uid, itemId, category, behavior, timestamp));
            }
        });

        SingleOutputStreamOperator<User> operator = flatMap.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<User>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(User user) {

                //  SimpleDateFormat simpleDateFormat = new SimpleDateFormat("")
                return user.timestamp;
            }
        });

        operator.filter(new FilterFunction<User>() {
            public boolean filter(User user) throws Exception {
                return user.behavior.equals("pv");
            }
        }).keyBy("itemId")
                .timeWindow(Time.seconds(60),Time.seconds(5))
                .aggregate(new CountAgg(),new WindowResult())
                .keyBy("windowEnd").process(new TopNItems(3)).print();

        env.execute();
    }

    public static class User {

        public String uid;
        public String itemId ;
        public String category;
        public String behavior;
        public Long timestamp;


        public User(String uid, String itemId, String category, String behavior, Long timestamp) {
            this.uid = uid;
            this.itemId = itemId;
            this.category = category;
            this.behavior = behavior;
            this.timestamp = timestamp;
        }

        public User() {

        }
        public static User of (String uid, String itemId, String category, String behavior, Long timestamp){
            return new User(uid,itemId,category,behavior,timestamp);
        }

        @Override
        public String toString() {
            return "User{" +
                    "uid='" + uid + '\'' +
                    ", itemId='" + itemId + '\'' +
                    ", category='" + category + '\'' +
                    ", behavior='" + behavior + '\'' +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }

    public static class CountAgg implements AggregateFunction<User, Long,Long > {

        public Long createAccumulator() {
            return 0L;
        }

        public Long add(User user, Long aLong) {
            return aLong+1;
        }

        public Long getResult(Long aLong) {
            return aLong;
        }

        public Long merge(Long aLong, Long acc1) {
            return aLong+acc1;
        }
    }

    public static class WindowResult implements WindowFunction<Long,ItemViewCount,Tuple,TimeWindow> { //输入，输出，key,time
//window

        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {

             String itemId = tuple.getField(0) ;
             Long aLong = iterable.iterator().next();
             Long timeEnd = timeWindow.getEnd();
             collector.collect(ItemViewCount.of(itemId,aLong,timeEnd));


        }
    }

    public static class ItemViewCount{

        public String itemId;
        public Long viewCount;
        public Long windowEnd;

        public ItemViewCount(String itemId, Long viewCount, Long windowEnd) {
            this.itemId = itemId;
            this.viewCount = viewCount;
            this.windowEnd = windowEnd;
        }

        public ItemViewCount() {
        }

        public static ItemViewCount of (String itemId, Long viewCount, Long windowEnd){
            return new ItemViewCount(itemId,viewCount,windowEnd);
        }

        @Override
        public String toString() {
            return "ItemViewCount{" +
                    "itemId='" + itemId + '\'' +
                    ", viewCount=" + viewCount +
                    ", windowEnd=" + windowEnd +
                    '}';
        }
    }


    public static class TopNItems extends KeyedProcessFunction<Tuple,ItemViewCount,String> {
        public int topSize;
        public TopNItems(int i) {
            topSize = i;
        }

        public  ListState<ItemViewCount> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //super.open(parameters);

            ListStateDescriptor<ItemViewCount> itemDes = new ListStateDescriptor<ItemViewCount>(
                    "item-State",
                    ItemViewCount.class
            );
            listState = getRuntimeContext().getListState(itemDes);

        }

        @Override
        public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {

            listState.add(itemViewCount);
            context.timerService().registerEventTimeTimer(itemViewCount.windowEnd+1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            List<ItemViewCount> itemViewCounts = new ArrayList<>();
            for (ItemViewCount item:listState.get()){
                itemViewCounts.add(item);
            }
            listState.clear();

            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return (int) (o1.viewCount - o2.viewCount);
                }
            });
            StringBuilder builder = new StringBuilder();

            builder.append("-------------------");
            builder.append("时间：").append(new Timestamp(timestamp-1)).append("\n");
            for (int i = 0; i < itemViewCounts.size()&&i<topSize; i++) {
                ItemViewCount currentItem = itemViewCounts.get(i);
                builder.append("No: ").append(i)
                        .append("商品ID  ").append(currentItem.itemId).append(": ")
                        .append("浏览量").append(currentItem.viewCount).append("\n");
            }
            out.collect(builder.toString());
        }
    }
}
