package com.cunliang.op;

import java.sql.Timestamp;

public class MyBehavior {
    public String userId;           // 用户ID
    public String itemId;           // 商品ID
    public String categoryId;       // 商品类目ID
    public String type;             // 用户行为, 包括("pv", "buy", "cart", "fav")
    public long timestamp;          // 行为发生的时间戳，单位秒
    public long counts = 1;

    public static MyBehavior of(String userId, String itemId, String categoryId, String type, long timestamp) {
        MyBehavior behavior = new MyBehavior();
        behavior.userId = userId;
        behavior.itemId = itemId;
        behavior.categoryId = categoryId;
        behavior.type = type;
        behavior.timestamp = timestamp;
        return behavior;
    }

    public static MyBehavior of(String userId, String itemId, String categoryId, String type, long timestamp,
                                long counts) {
        MyBehavior behavior = new MyBehavior();
        behavior.userId = userId;
        behavior.itemId = itemId;
        behavior.categoryId = categoryId;
        behavior.type = type;
        behavior.timestamp = timestamp;
        behavior.counts = counts;
        return behavior;
    }

    @Override
    public String toString() {
        return "MyBehavior{" + "userId='" + userId + '\'' + ", itemId='" + itemId + '\''
                + ", categoryId='" + categoryId + '\'' + ", type='" + type + '\''
                + ", timestamp=" + timestamp + "," + new Timestamp(timestamp)
                + "counts=" + counts + '}';
    }

    public String getUserId() {
        return userId;
    }
    public String getItemId() {
        return itemId;
    }
    public String getCategoryId() {
        return categoryId;
    }
    public String getType() {
        return type;
    }
    public long getTimestamp() {
        return timestamp;
    }
    public long getCounts() {
        return counts;
    }
}