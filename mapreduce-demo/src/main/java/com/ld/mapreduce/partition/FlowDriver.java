package com.ld.mapreduce.partition;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author:ld
 * @create:2019-01-23 17:19
 * @description: 流量统计 驱动类
 * 对文件截取，统计输出 手机号 上行流量 下行流量 总流量  (自定义分区)
 */
public class FlowDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"e:/input/partition","e:/partition"};
        //1. 获取job对象
        Job job = Job.getInstance();

        //2. 设置jar包类路径
        job.setJarByClass(FlowDriver.class);

        //3. 设置mapper和reducer类路径
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //4. 设置map输出参数类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //5. 设置最终输出参数类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        /**
         * 设置partitioner类路径
         */
        job.setPartitionerClass(ProvincePartitioner.class);
        //设置分区个数
        job.setNumReduceTasks(5);

        //6. 设置输入输出文件路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //7. 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
