package com.ld.mapreduce.sort.partsort;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author:ld
 * @create:2019-02-01 14:28
 * @description:
 */
public class PartSortDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"e:/input/serializable","e:/partsort"};
        //1. 获取job对象
        Job job = Job.getInstance();

        //2. 设置jar包类路径
        job.setJarByClass(PartSortDriver.class);

        //3. 设置mapper和reducer类路径
        job.setMapperClass(PartSortMapper.class);
        job.setReducerClass(PartSortReducer.class);

        //4. 设置map输出参数类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //5. 设置最终输出参数类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        /**
         * 设置分区
         */
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);

        //6. 设置输入输出文件路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //7. 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
