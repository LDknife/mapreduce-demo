package com.ld.mapreduce.input.nline;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author:ld
 * @create:2019-01-23 16:34
 * @description: 单词统计Mapper阶段
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    //输出key
    private Text k = new Text();
    //输出value
    private IntWritable v = new IntWritable(1);

    /**
     * map阶段的业务逻辑
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1. 取出一行数据,并转化为字符串
        String line = value.toString();

        //2. 截取字符串
        String[] fields = line.split(" ");

        //3. 输出数据
        for(String field : fields){
            //格式化数据
            k.set(field);
            context.write(k,v);
        }
    }
}
