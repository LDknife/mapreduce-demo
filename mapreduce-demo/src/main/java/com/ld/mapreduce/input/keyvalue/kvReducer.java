package com.ld.mapreduce.input.keyvalue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author:ld
 * @create:2019-01-31 16:17
 * @description:
 */
public class kvReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable value = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        //1.循环遍历 累加
        int sum = 0;
        for(IntWritable value: values){
            sum += value.get();
        }

        value.set(sum);
        //2. 写出
        context.write(key,value);
    }
}
