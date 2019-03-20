package com.lyf.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.util.Date;

/**
 * @author lyf
 * @date 2019/3/18 0018 下午 7:58
 */
public class HadoopDriverUtilPro{

    private String jobName;
    private Class map;
    private Class reduce;
    private Class main;
    private String outPath;

    /**
     * 自动生成任务编号
     */
    public HadoopDriverUtilPro() {
        this.jobName = "job_" + new Date().getTime();
    }

    /**
     * 设置运行类
     * @param map   Mapper类
     * @param reducer   Reduce类
     * @param main  运行主类
     */
    public HadoopDriverUtilPro(Class map, Class reducer, Class main) {
        this.jobName = "job_" + new Date().getTime();
        this.map = map;
        this.reduce = reducer;
        this.main = main;
    }

    /**
     * 设置输入输出路径
     * @param in    文件输入路径
     * @param out   文件输出路径
     * @return
     * @throws IOException
     */
    public Job getInstance(String in, String out) throws IOException {
        // 1. 获取配置
        Configuration conf = new Configuration();
        // 2. 获取job对象
        Job job = Job.getInstance(conf, jobName);
        // 3. 设置运行主类
        job.setJarByClass(main);
        // 4. 设置Map
        job.setMapperClass(map);
        // 获取泛型列表
        ParameterizedType p = (ParameterizedType) map.getGenericSuperclass();
        // 获取map阶段的输出类型并设置
        job.setMapOutputKeyClass((Class) p.getActualTypeArguments()[2]);
        job.setMapOutputValueClass((Class) p.getActualTypeArguments()[3]);
        FileInputFormat.addInputPath(job, new Path(in));

        // 5. 设置Reduce
        job.setReducerClass(reduce);
        // 获取reduce阶段的输出类型并设置
        p = (ParameterizedType) reduce.getGenericSuperclass();
        job.setOutputKeyClass((Class) p.getActualTypeArguments()[2]);
        job.setOutputValueClass((Class) p.getActualTypeArguments()[3]);
        // 6. 随机生成输出目录,方便调试
        this.outPath = out+"\\"+r(4);
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        return job;
    }

    /**
     * 生成随机数
     * @param len 随机数长度
     * @return
     */
    public String r(int len) {
        return (Math.random()+"").substring(2,len+2);
    }

    public void close(Job job) throws InterruptedException, IOException, ClassNotFoundException {
        // 提交job
        int isok = job.waitForCompletion(true) ? 0 : 1;
        System.out.println("---------输出目录-----------");
        System.out.println("outpath: "+outPath);
        // 退出
        System.exit(isok);
    }
}
