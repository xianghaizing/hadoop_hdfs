package com.lyf.mr01;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @author lyf
 * @date 2019/3/17 9:23
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    	// 1. 获取配置
		Configuration conf = new Configuration();
		// 2. 获取job对象
		Job job = Job.getInstance(conf, "mywordcount");
		// 3. 设置运行主类
		job.setJarByClass(WordCountDriver.class);
		// 4. 设置Map
		job.setMapperClass(WordCountMapDemo.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// 5. 设置Reduce
		job.setReducerClass(WordCountReduceDemo.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 6. 提交job
		int isok = job.waitForCompletion(true)?0:1;
		// 7. 退出
		System.exit(isok);
	}
}
