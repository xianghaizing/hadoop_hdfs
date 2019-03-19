package com.lyf.bo;

import com.lyf.bean.FlowCompareBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author lyf
 * @date 2019/3/17 0017 下午 8:11
 */
public class ProvinceComparePartitioner extends Partitioner<FlowCompareBean, Text> {
    @Override
    public int getPartition(FlowCompareBean flowCompareBean, Text text, int numPartitions) {
        // 获取手机号第三位
        String pre3Num = text.toString().substring(2,3);
        return Integer.parseInt(pre3Num);
    }
}
