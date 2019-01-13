package com.ld.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KVReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    private IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        //1 、统计总数
        int sum = 0;
        for (IntWritable value : values){
            sum += value.get();
        }
        v.set(sum);
        //2 、写出
        context.write(key,v);
    }
}
