package com.ld.mapreduce.input.combine;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author:ld
 * @create:2019-01-23 16:43
 * @description: 单词统计reducer阶段
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * reduce阶段业务逻辑
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        //1. 统计单词总数
        int sum = 0;

        for(IntWritable value: values){
            sum += value.get();
        }

        //2.输出数据
        context.write(key,new IntWritable(sum));
    }
}
