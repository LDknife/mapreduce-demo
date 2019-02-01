package com.ld.mapreduce.input.defined;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author:ld
 * @create:2019-02-01 10:39
 * @description: 整合小文件map阶段
 */
public class SequenceFileMapper extends Mapper<Text, BytesWritable,Text, BytesWritable> {

    /**
     * map阶段业务代码
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(key,value);
    }
}
