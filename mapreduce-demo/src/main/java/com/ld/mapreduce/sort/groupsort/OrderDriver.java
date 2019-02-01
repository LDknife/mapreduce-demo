package com.ld.mapreduce.sort.groupsort;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author:ld
 * @create:2019-02-01 15:13
 * @description:
 */
public class OrderDriver {

    public static void main(String[] args) throws Exception {
        args = new String[]{"e:/input/group","e:/group"};
        //1. 获取job对象
        Job job = Job.getInstance();

        //2. 设置jar包类路径
        job.setJarByClass(OrderDriver.class);

        //3. 设置mapper和reducer类路径
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        //4. 设置map输出参数类型
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //5. 设置最终输出参数类型
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        /**
         * 设置分组排序
         */
        job.setGroupingComparatorClass(OrderGroupSort.class);

        //6. 设置输入输出文件路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //7. 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
