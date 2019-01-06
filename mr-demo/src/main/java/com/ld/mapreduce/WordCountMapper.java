package com.ld.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * map task
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    private IntWritable i = new IntWritable(1);
    private Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.获取一行数据
        String line = value.toString();
        //2.切割
        String[] words = line.split(" ");
        //3.输出
        for (String word : words) {
            k.set(word);
            context.write(k,i);
        }
    }
}
