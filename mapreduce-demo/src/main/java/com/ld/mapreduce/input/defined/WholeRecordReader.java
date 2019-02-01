package com.ld.mapreduce.input.defined;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author:ld
 * @create:2019-02-01 10:08
 * @description: 自定义RecordReader 一次读取一个文件
 */
public class WholeRecordReader extends RecordReader<Text, BytesWritable> {

    //切片对象
    private FileSplit fileSplit;
    //配置信息
    private Configuration conf;

    //key对象
    private Text key = new Text();
    //value对象
    private BytesWritable value = new BytesWritable();

    //进度
    private boolean progress = false;

    /**
     * 初始化方法
     * @param inputSplit
     * @param taskAttemptContext
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        fileSplit = (FileSplit) inputSplit;
        conf = taskAttemptContext.getConfiguration();
    }

    /**
     * 读取数据的业务方法
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if(!progress){
            //1. 读取数据
            //获取输入文件的路径
            Path path = fileSplit.getPath();
            //获取文件系统
            FileSystem fileSystem = path.getFileSystem(conf);
            //获取输入流对象
            FSDataInputStream fis = fileSystem.open(path);
            //缓存区
            byte[] content = new byte[(int)fileSplit.getLength()];
            //读取数据到缓存区
            IOUtils.readFully(fis,content,0,content.length);
            //2. 设置值到输出key和value中
            key.set(path.toString());
            value.set(content,0,content.length);

            //设置进度为true
            progress = true;
            return true;
        }
        return false;
    }

    /**
     * 获取key
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    /**
     * 获取value
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * 获取进度
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return progress ? 1 : 0;
    }

    @Override
    public void close() throws IOException {

    }
}
