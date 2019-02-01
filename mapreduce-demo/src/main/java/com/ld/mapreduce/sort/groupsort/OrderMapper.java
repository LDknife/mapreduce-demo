package com.ld.mapreduce.sort.groupsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author:ld
 * @create:2019-02-01 14:53
 * @description:
 */
public class OrderMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {

    private OrderBean k = new OrderBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //0000001 Pdt_01 222.8
        //1. 获取一行数据
        String line = value.toString();
        //2. 截取数据
        String[] fields = line.split(" ");
        //3. 整合数据
        k.setOrderId(Integer.parseInt(fields[0]));
        k.setPrice(Double.parseDouble(fields[2]));

        //4. 写出
        context.write(k,NullWritable.get());
    }
}
