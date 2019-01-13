package com.ld.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author:ld
 * @create:2019-01-10 10:47
 * @description: Mapper
 */
public class FlowSumMapper extends Mapper<LongWritable,Text, Text,FlowBean> {

    private Text k;
    private FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1 获取一行数据
        String line = value.toString();
        //2 切割数据
        String[] fields = line.split(" ");
        //3 生成对象
        k = new Text(fields[1]);
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);
        v.set(upFlow,downFlow);
        //4 写出
        context.write(k,v);
    }
}
