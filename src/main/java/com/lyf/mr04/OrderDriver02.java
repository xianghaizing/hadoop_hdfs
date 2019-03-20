package com.lyf.mr04;

import com.lyf.bo.OrderGroupCompare;
import com.lyf.bo.OrderPartitioner;
import com.lyf.utils.HadoopDriverUtilPro;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @author lyf
 * @date 2019/3/18 0018 下午 9:56
 */
public class OrderDriver02{



    public static void main(String []args) throws IOException, ClassNotFoundException, InterruptedException {

        HadoopDriverUtilPro util = new HadoopDriverUtilPro(OrderMap.class, OrderReduce.class, OrderDriver02.class);

        Job job = util.getInstance("E:\\xianghaizing\\hadoop_hdfs\\input\\01","E:\\xianghaizing\\hadoop_hdfs\\output");

        //设置分区
        job.setPartitionerClass(OrderPartitioner.class);
        job.setNumReduceTasks(3);
        //设置分组
        job.setGroupingComparatorClass(OrderGroupCompare.class);


        util.close(job);
    }
}
