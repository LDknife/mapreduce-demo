package com.ld.mapreduce.sort.groupsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author:ld
 * @create:2019-02-01 14:59
 * @description: 分组排序
 */
public class OrderGroupSort extends WritableComparator {

    //创建实例
    protected OrderGroupSort(){
        super(OrderBean.class,true);
    }

    /**
     * 分组排序
     * @param a
     * @param b
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aOrderBean = (OrderBean)a;
        OrderBean bOrderBean = (OrderBean)b;

        //进行分组
        if(aOrderBean.getOrderId() > bOrderBean.getOrderId()){
            return 1;
        }else if(aOrderBean.getOrderId() < bOrderBean.getOrderId()){
            return -1;
        }else{
            return 0;
        }
    }
}
