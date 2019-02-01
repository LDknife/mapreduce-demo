package com.ld.mapreduce.sort.partsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author:ld
 * @create:2019-02-01 11:49
 * @description: 自定义分区，按省份分区
 */
public class ProvincePartitioner extends Partitioner<FlowBean, Text> {

    /**
     * 分区
     * @param text
     * @param flowBean
     * @param i
     * @return
     */
    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {
        
        //1.获取key
        String key = text.toString();
        
        //2. 截取前三位
        String prefix = key.substring(0, 3);

        int result = 4;
        //3. 判断
        if("136".equals(prefix)){//136
            result = 0;
        }else if("137".equals(prefix)){
            result = 1;
        }else if("138".equals(prefix)){
            result = 2;
        }else if("139".equals(prefix)){
            result = 3;
        }else{
            result = 4;
        }
        return result;
    }
}
