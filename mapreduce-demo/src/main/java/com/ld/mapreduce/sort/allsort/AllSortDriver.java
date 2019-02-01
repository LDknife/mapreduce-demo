package com.ld.mapreduce.sort.allsort;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author:ld
 * @create:2019-02-01 14:16
 * @description:
 */
public class AllSortDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"e:/input/serializable","e:/allsort"};
        //1. 获取job对象
        Job job = Job.getInstance();

        //2. 设置jar包类路径
        job.setJarByClass(AllSortDriver.class);

        //3. 设置mapper和reducer类路径
        job.setMapperClass(AllSortMapper.class);
        job.setReducerClass(AllSortReudcer.class);

        //4. 设置map输出参数类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //5. 设置最终输出参数类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //6. 设置输入输出文件路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //7. 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
