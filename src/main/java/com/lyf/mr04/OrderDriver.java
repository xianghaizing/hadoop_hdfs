package com.lyf.mr04;

import com.lyf.bean.OrderBean;
import com.lyf.bean.OrderGroupCompare;
import com.lyf.bean.OrderPartitioner;
import com.lyf.utils.HadoopDriverUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @author lyf
 * @date 2019/3/18 0018 下午 9:56
 */
public class OrderDriver {
    public static void main(String []args) throws IOException, ClassNotFoundException, InterruptedException {
        HadoopDriverUtil util = new HadoopDriverUtil();
        util.setMap(OrderMap.class, OrderBean.class, NullWritable.class);
        util.setReducer(OrderReduce.class, NullWritable.class, OrderBean.class);
        Job job = util.getInstance(OrderDriver.class,
                "E:\\xianghaizing\\hadoop_hdfs\\input\\01",
                "E:\\xianghaizing\\hadoop_hdfs\\output");

        job.setGroupingComparatorClass(OrderGroupCompare.class);

        job.setPartitionerClass(OrderPartitioner.class);
        job.setNumReduceTasks(3);

        util.close(job);
    }
}
