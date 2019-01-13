package com.ld.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1.获取配置信息
        Job job = Job.getInstance();

        //2.设置jar加载路径
        job.setJarByClass(WordCountDriver.class);

        //3.设置map和reduce 类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //4.设置map的输出参数
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5.设置reduce的输出参数
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        //8 把TextInputFormat 设置为 CombineTextInputFormat  为设置多个小文件为一个切片
        //job.setInputFormatClass(CombineTextInputFormat.class);
        //CombineTextInputFormat.setMinInputSplitSize(job,2097152);
        //CombineTextInputFormat.setMaxInputSplitSize(job,4194304);
        
        // 把TextInputFormat 设置为 NLineInputFormat 为设置多少行为一个切片
        //job.setInputFormatClass(NLineInputFormat.class);
        //NLineInputFormat.setNumLinesPerSplit(job,2);

        //6.设置输入和输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //7.提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
