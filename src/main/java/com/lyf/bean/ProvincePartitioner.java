package com.lyf.bean;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author lyf
 * @date 2019/3/17 0017 下午 8:11
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        // 获取手机号第三位
        String pre3Num = text.toString().substring(2,3);
        return Integer.parseInt(pre3Num);
    }
}
