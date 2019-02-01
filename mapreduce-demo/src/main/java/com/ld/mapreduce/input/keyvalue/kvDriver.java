package com.ld.mapreduce.input.keyvalue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author:ld
 * @create:2019-01-31 16:20
 * @description: 测试keyvalueTextInputFormat
 * 统计key的个数
 */
public class kvDriver {

    public static void main(String[] args) throws Exception {
        args = new String[]{"e:/input/keyvalue","e:/output"};
        //1. 获取job对象
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        /**
         * 设置KeyValueLineRecordReader
         */
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR,"-");
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        //2. 设置jar包类路径
        job.setJarByClass(kvDriver.class);
        //3. 设置mapper和reducer 类路径
        job.setMapperClass(kvMapper.class);
        job.setReducerClass(kvReducer.class);

        //4. 设置map的输出参数类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5. 设置最终的输出参数类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6. 设置文件输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //7. 提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
