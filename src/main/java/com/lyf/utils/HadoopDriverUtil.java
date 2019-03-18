package com.lyf.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Date;

/**
 * @author lyf
 * @date 2019/3/18 0018 下午 7:58
 */
public class HadoopDriverUtil {

    private String jobName;

    private Class mapClass;
    private Class outKeyClass;
    private Class outValueClass;

    private Class reducerClass;
    private Class inKeyClass;
    private Class inValueClass;

    private String outPath;

    public HadoopDriverUtil() {
        this.jobName = "job_" + new Date().getTime();
    }

    public void setMap(Class map, Class key, Class value) {
        this.mapClass = map;
        this.outKeyClass = key;
        this.outValueClass = value;
    }

    public void setReducer(Class reducer, Class key, Class value) {
        this.reducerClass = reducer;
        this.inKeyClass = key;
        this.inValueClass = value;
    }

    public Job getInstance(Class main, String in, String out) throws IOException {
        // 1. 获取配置
        Configuration conf = new Configuration();
        // 2. 获取job对象
        Job job = Job.getInstance(conf, jobName);
        // 3. 设置运行主类
        job.setJarByClass(main);

        // 4. 设置Map
        job.setMapperClass(mapClass);
        job.setMapOutputKeyClass(outKeyClass);
        job.setMapOutputValueClass(outValueClass);
        FileInputFormat.addInputPath(job, new Path(in));

        // 5. 设置Reduce
        job.setReducerClass(reducerClass);
        job.setOutputKeyClass(inKeyClass);
        job.setOutputValueClass(inValueClass);
        this.outPath = out+"\\"+r(4);
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        return job;

//        // 设置分区
//        job.setPartitionerClass(ProvinceComparePartitioner.class);
//        job.setNumReduceTasks(10);
    }

    public String r(int len) {
        return (Math.random()+"").substring(2,len+2);
    }

    public void close(Job job) throws InterruptedException, IOException, ClassNotFoundException {
        // 6. 提交job
        int isok = job.waitForCompletion(true) ? 0 : 1;
        System.out.println("--------------------");
        System.out.println("outpath: "+outPath);
        // 7. 退出
        System.exit(isok);
    }
}
