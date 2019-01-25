package com.ld.mapreduce.serializable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author:ld
 * @create:2019-01-23 17:05
 * @description: 流量实体类（序列化）
 */
public class FlowBean implements Writable {

    //上行流量
    private double upFlow;
    //下行流量
    private double downFlow;
    //总流量
    private double totalFlow;

    /**
     * 空参构造必须有,反序列化是用
     */
    public FlowBean() {
    }

    /**
     * 序列化
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(upFlow);
        dataOutput.writeDouble(downFlow);
        dataOutput.writeDouble(totalFlow);
    }

    /**
     * 反序列化 顺序必须和序列化一致
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readDouble();
        downFlow = dataInput.readDouble();
        totalFlow = dataInput.readDouble();
    }

    public double getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(double upFlow) {
        this.upFlow = upFlow;
    }

    public double getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(double downFlow) {
        this.downFlow = downFlow;
    }

    public double getTotalFlow() {
        return totalFlow;
    }

    public void setTotalFlow(double totalFlow) {
        this.totalFlow = totalFlow;
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + totalFlow;
    }

    /**
     * 赋值并计算
     * @param upFlow
     * @param downFlow
     */
    public void set(double upFlow,double downFlow){
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        totalFlow = upFlow + downFlow;
    }
}
