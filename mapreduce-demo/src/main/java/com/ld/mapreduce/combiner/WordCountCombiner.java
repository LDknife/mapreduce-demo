package com.ld.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author:ld
 * @create:2019-02-01 15:26
 * @description: 对每一个maptask 进行汇总
 */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //1 汇总
        int sum = 0;
        for(IntWritable value: values){
            sum += value.get();
        }

        //2. 写出
        context.write(key,new IntWritable(sum));
    }
}
