package com.ld.mapreduce.sort.allsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author:ld
 * @create:2019-02-01 14:07
 * @description:
 */
public class AllSortMapper extends Mapper<LongWritable, Text, FlowBean,Text> {

    private FlowBean k = new FlowBean();
    private Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1. 获取一行数据
        String line = value.toString();

        //2. 截取
        String[] fields = line.split(" ");

        //3. 整合数据
        double upFlow = Double.parseDouble(fields[fields.length - 3]);
        double downFlow = Double.parseDouble(fields[fields.length - 2]);
        k.set(upFlow,downFlow);
        v.set(fields[1]);

        //4. 写出
        context.write(k,v);
    }
}
