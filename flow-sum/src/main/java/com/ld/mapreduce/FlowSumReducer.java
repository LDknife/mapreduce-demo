package com.ld.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author:ld
 * @create:2019-01-10 10:58
 * @description: reducer
 */
public class FlowSumReducer extends Reducer<Text,FlowBean, Text,FlowBean> {

    private FlowBean value = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        //1 汇总数据

        long upFlowSum = 0;
        long downFlowSum = 0;
        for(FlowBean flowBean : values){
            upFlowSum += flowBean.getUpFlow();
            downFlowSum += flowBean.getDownFlow();
        }
        value.set(upFlowSum,downFlowSum);
        //2 写出
        context.write(key,value);
    }
}
