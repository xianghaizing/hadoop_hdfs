package com.lyf.mr03;

import com.lyf.bean.FlowCompareBean;
import com.lyf.bean.ProvinceComparePartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author lyf
 * @date 2019/3/17 0017 下午 7:24
 */
public class FlowCountComparableDemo {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1. 获取配置
        Configuration conf = new Configuration();
        // 2. 获取job对象
        Job job = Job.getInstance(conf, "myflowcount");
        // 3. 设置运行主类
        job.setJarByClass(FlowCountComparableDemo.class);

        // 4. 设置Map
        job.setMapperClass(FlowCountCompareMap.class);
        job.setMapOutputKeyClass(FlowCompareBean.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // 5. 设置Reduce
        job.setReducerClass(FlowCountCompareReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowCompareBean.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置分区
        job.setPartitionerClass(ProvinceComparePartitioner.class);
        job.setNumReduceTasks(10);

        // 6. 提交job
        int isok = job.waitForCompletion(true) ? 0 : 1;
        // 7. 退出
        System.exit(isok);
    }
}
