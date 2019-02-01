package com.ld.mapreduce.output;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author:ld
 * @create:2019-02-01 16:06
 * @description: 判断每行log中是否包含ld,包含输出到ld.log,不包含输出到other.log
 */
public class FilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        line += "\r\n";
        context.write(new Text(line),NullWritable.get());
    }
}
