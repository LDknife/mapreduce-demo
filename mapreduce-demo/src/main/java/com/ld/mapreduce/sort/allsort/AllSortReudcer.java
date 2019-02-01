package com.ld.mapreduce.sort.allsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author:ld
 * @create:2019-02-01 14:14
 * @description:
 */
public class AllSortReudcer extends Reducer<FlowBean, Text, Text, FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //写出
        for(Text value: values){
            context.write(value,key);
        }

    }
}
