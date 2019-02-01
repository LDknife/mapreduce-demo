package com.ld.mapreduce.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author:ld
 * @create:2019-01-23 17:13
 * @description: mapper 阶段
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    //输出key
    private Text k = new Text();
    //输出value
    private FlowBean v = new FlowBean();
    /**
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

//        1213 13723546789 127.0.0.1 www.baidu.com 230 399 200
//        2123 13687398278 193.168.8.3   278 280 200
        //1. 获取一行数据
        String line = value.toString();
        //2. 截取
        String[] fields = line.split(" ");

        //3. 整合数据
        k.set(fields[1]);

        //上行流量
        double upFlow = Double.parseDouble(fields[fields.length - 3]);
        //下行流量
        double downFlow = Double.parseDouble(fields[fields.length - 2]);
        //赋值
        v.set(upFlow,downFlow);

        //4.输出
        context.write(k,v);
    }
}
