package com.lyf.bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author lyf
 * @date 2019/3/18 0018 下午 9:22
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private int orderId;
    private String goodsId;
    private Double price;

    public OrderBean() {
        super();
    }

    public OrderBean(int orderId, String goodsId, Double price) {
        super();
        this.orderId = orderId;
        this.goodsId = goodsId;
        this.price = price;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.orderId);
        out.writeUTF(this.goodsId);
        out.writeDouble(this.price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readInt();
        this.goodsId = in.readUTF();
        this.price = in.readDouble();
    }

    @Override
    public String toString() {
        return orderId +"\t"+ goodsId + '\t' + price;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public String getGoodsId() {
        return goodsId;
    }

    public void setGoodsId(String goodsId) {
        this.goodsId = goodsId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean o) {
//        int result;
//        if(orderId>o.getOrderId()){
//            result = 1;
//        }else if(orderId<o.getOrderId()){
//            result = -1;
//        }else {
//            result = this.price>o.getPrice() ? -1 : 1;
//        }
        return this.price>o.getPrice() ? -1 : 1;
    }
}
