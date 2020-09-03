package com.cunliang.op;

import java.sql.Timestamp;

public class ItemViewCount {
    public String itemId;     // 商品ID
    public String type;     // 事件类型
    public long windowStart;  // 窗口开始时间戳
    public long windowEnd;  // 窗口结束时间戳
    public long viewCount;  // 商品的点击量

    public static ItemViewCount of(String itemId, String type, long windowStart, long windowEnd, long viewCount) {
        ItemViewCount result = new ItemViewCount();
        result.itemId = itemId;
        result.type = type;
        result.windowStart = windowStart;
        result.windowEnd = windowEnd;
        result.viewCount = viewCount;
        return result;
    }

    @Override
    public String toString() {
        return "{" +
                "itemId='" + itemId + '\'' +
                "type='" + type + '\'' +
                ", windowStart=" + windowStart + " , " + new Timestamp(windowStart) +
                ", windowEnd=" + windowEnd + " , " + new Timestamp(windowEnd) +
                ", viewCount=" + viewCount +
                '}';
    }
}