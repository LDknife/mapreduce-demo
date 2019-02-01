package com.ld.mapreduce.output;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author:ld
 * @create:2019-02-01 16:10
 * @description:
 */
public class FilterDriver {

    public static void main(String[] args) throws Exception {
        args = new String[]{"e:/input/output","e:/output"};
        Job job = Job.getInstance();

        job.setJarByClass(FilterDriver.class);

        job.setMapperClass(FilterMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        /**
         * 设置输出
         */
        job.setOutputFormatClass(FilterOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
