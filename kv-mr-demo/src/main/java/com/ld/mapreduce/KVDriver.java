package com.ld.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * hadoop hello world
 * hadoop hello world
 * spark hello world
 * spark hello world
 *
 * 统计开头单词的总数
 */
public class KVDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[] {"c:/test/kv.txt","c:/test/kv"};
        //1 获取job对象
        Configuration conf = new Configuration();
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," ");
        Job job = Job.getInstance(conf);
        //2 设置jar路径
        job.setJarByClass(KVDriver.class);
        //3 设置mapper 和 reducer路径
        job.setMapperClass(KVMapper.class);
        job.setReducerClass(KVReducer.class);

        //4 设置mapper输出参数类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5 设置最终的输出参数类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置InputFormat
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        //6 设置输入输出文件路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //7 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
