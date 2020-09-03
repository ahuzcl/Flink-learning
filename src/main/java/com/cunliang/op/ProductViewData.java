package com.cunliang.op;

public class ProductViewData {

    public String productID;
    public String userID;
    public Long operation;
    public Long timeStamp;

    public String getProductID() {
        return productID;
    }

    public void setProductID(String productID) {
        this.productID = productID;
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public Long getOperation() {
        return operation;
    }

    public void setOperation(Long operation) {
        this.operation = operation;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public ProductViewData(String productID, String userID, Long operation, Long timeStamp) {
        this.productID = productID;
        this.userID = userID;
        this.operation = operation;
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "ProductViewData{" +
                "productID='" + productID + '\'' +
                ", userID='" + userID + '\'' +
                ", operation=" + operation +
                ", timeStamp=" + timeStamp +
                '}';
    }
}
