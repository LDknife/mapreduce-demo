package com.ld.mapreduce.sort.groupsort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author:ld
 * @create:2019-02-01 14:46
 * @description:
 */
public class OrderBean implements WritableComparable<OrderBean> {

    //订单id
    private int orderId;
    //价格
    private double price;

    public OrderBean() {
    }


    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return  orderId + "\t" + price;
    }

    /**
     * 二次排序
     * @param o
     * @return
     */
    @Override
    public int compareTo(OrderBean o) {
        //对订单号进行正序排序
        if(this.orderId > o.getOrderId()){
            return 1;
        }else if(this.orderId < o.getOrderId()){
            return -1;
        }else{
            //对价格进行倒序排序
            return this.price > o.getPrice() ? -1 : 1;
        }
    }

    /**
     * 序列化
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(orderId);
        dataOutput.writeDouble(price);
    }

    /**
     * 反序列化
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        orderId = dataInput.readInt();
        price = dataInput.readDouble();
    }
}
