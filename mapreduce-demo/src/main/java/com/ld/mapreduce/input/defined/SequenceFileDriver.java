package com.ld.mapreduce.input.defined;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * @author:ld
 * @create:2019-02-01 10:53
 * @description: 自定义inputFormat, 整合多个小文件
 */
public class SequenceFileDriver {

    public static void main(String[] args) throws Exception {
        args = new String[]{"e:/input/defined","e:/defined"};
        //1. 获取job对象
        Job job = Job.getInstance();
        //2. 设置jar类路径
        job.setJarByClass(SequenceFileDriver.class);

        //3. 设置mapper和reducer类路径
        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);

        //4. 设置map输出参数类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        //5. 设置最终输出参数类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        /**
         * 设置inputFormat
         */
        job.setInputFormatClass(WholeFileInputFormat.class);
        /**
         * 设置outputFormat
         */
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        //6. 设置输入输出文件路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //7 提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
