package com.ld.mapreduce.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author:ld
 * @create:2019-01-23 17:18
 * @description: reduce 阶段
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    //输出的value
    private FlowBean v = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        //1. 循环遍历value,进行累加

        //上行总流量
        double sumUpFlow = 0;
        //下行总流量
        double sumDownFlow = 0;

        for(FlowBean value: values){
            sumUpFlow += value.getUpFlow();
            sumDownFlow += value.getDownFlow();
        }

        //整合
        v.set(sumUpFlow,sumDownFlow);

        //2. 输出
        context.write(key,v);
    }
}
