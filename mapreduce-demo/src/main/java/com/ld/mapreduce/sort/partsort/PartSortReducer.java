package com.ld.mapreduce.sort.partsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author:ld
 * @create:2019-02-01 14:26
 * @description:
 */
public class PartSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //写出
        for(Text value: values){
            context.write(value,key);
        }

    }
}
