package com.lyf.bo;

import com.lyf.bean.OrderBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * @author lyf
 * @date 2019/3/17 0017 下午 8:11
 */
public class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean key, NullWritable text, int numPartitions) {
        return key.getOrderId() % numPartitions;
    }
}
