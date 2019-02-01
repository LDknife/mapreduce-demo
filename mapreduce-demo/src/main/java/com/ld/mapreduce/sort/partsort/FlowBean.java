package com.ld.mapreduce.sort.partsort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author:ld
 * @create:2019-02-01 14:00
 * @description: 实现排序接口
 */
public class FlowBean implements WritableComparable<FlowBean> {

    //上行流量
    private double upFlow;
    //下行流量
    private double downFlow;
    //总流量
    private double totalFlow;

    public FlowBean() {
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
     * 排序
     * @param o
     * @return
     */
    @Override
    public int compareTo(FlowBean o) {
        //倒序排序
        return this.totalFlow > o.totalFlow ? -1 : 1;
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
     * 反序列化
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readDouble();
        downFlow = dataInput.readDouble();
        totalFlow = dataInput.readDouble();
    }

    /**
     * 设置数据
     * @param upFlow
     * @param downFlow
     */
    public void set(double upFlow,double downFlow){
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        totalFlow = upFlow + downFlow;
    }
}
