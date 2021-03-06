package com.ld.mapreduce;

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

public class WholeRecordReader extends RecordReader<Text, BytesWritable> {


    private FileSplit split;
    private Configuration configuration;

    private Text k = new Text();
    private BytesWritable v = new BytesWritable();

    private boolean flag = true;
    /**
     * 初始化数据
     * @param inputSplit
     * @param taskAttemptContext
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.split = (FileSplit) inputSplit;
        configuration = taskAttemptContext.getConfiguration();
    }

    /**
     * 核心业务逻辑
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if(flag){
            byte[] buf = new byte[(int)split.getLength()];
            //1 获取fs对象
            Path path = split.getPath();
            FileSystem fs = path.getFileSystem(configuration);

            //2 获取输入流
            FSDataInputStream fis = fs.open(path);

            //3 拷贝数据
            IOUtils.readFully(fis,buf,0,buf.length);

            //4 设置value
            v.set(buf,0,buf.length);

            //5 设置key
            k.set(path.toString());

            //6 关闭资源
            IOUtils.closeStream(fis);

            flag = false;

            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
