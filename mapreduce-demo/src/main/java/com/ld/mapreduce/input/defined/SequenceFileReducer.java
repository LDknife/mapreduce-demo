package com.ld.mapreduce.input.defined;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author:ld
 * @create:2019-02-01 11:07
 * @description:
 */
public class SequenceFileReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {

    /**
     * reduce
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        //写出
        context.write(key,values.iterator().next());
    }
}
