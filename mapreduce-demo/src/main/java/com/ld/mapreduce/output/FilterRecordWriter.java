package com.ld.mapreduce.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author:ld
 * @create:2019-02-01 15:57
 * @description:
 */
public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {


    private FSDataOutputStream ldFos;
    private FSDataOutputStream otherFos;
    /**
     * 初始化
     * @param context
     */
    protected FilterRecordWriter(TaskAttemptContext context){

        //获取配置对象
        Configuration conf = context.getConfiguration();

        //获取文件对象
        try {
            FileSystem fs = FileSystem.get(conf);
            //创建输出文件路径
            Path ldPath = new Path("e:/ld.log");
            Path otherPath = new Path("e:/other.log");

            //创建输出流
            ldFos = fs.create(ldPath);
            otherFos = fs.create(otherPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 写数据
     * @param text
     * @param nullWritable
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {

        //获取数据
        String line = text.toString();

        //判断
        if(line.contains("ld")){
            ldFos.write(line.getBytes());
        }else{
            otherFos.write(line.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        ldFos.close();
        otherFos.close();
    }
}
