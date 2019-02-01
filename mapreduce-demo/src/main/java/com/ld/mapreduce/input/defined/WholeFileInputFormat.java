package com.ld.mapreduce.input.defined;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @author:ld
 * @create:2019-02-01 10:03
 * @description: 自定义FileInputFormat 一次读取一个文件
 */
public class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable> {

    /**
     * 切片方法
     */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        //不进行切片
        return false;
    }

    /**
     * 读取数据方法
     * @param inputSplit
     * @param taskAttemptContext
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //创建recordReader对象
        WholeRecordReader recordReader = new WholeRecordReader();
        //调用初始化方法
        recordReader.initialize(inputSplit,taskAttemptContext);
        return recordReader;
    }
}
