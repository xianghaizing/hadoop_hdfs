package com.lyf.mr06;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream addOut = null;
    FSDataOutputStream delOut = null;

    public FilterRecordWriter(TaskAttemptContext job) {
// 1 获取文件系统
        FileSystem fs;
        try {
            fs = FileSystem.get(job.getConfiguration());
// 2 创建输出文件路径
            Path addPath = new Path("E:\\hadoop_hdfs\\input\\03\\add.log");
            Path delPath = new Path("E:\\hadoop_hdfs\\input\\03\\del.log");
// 3 创建输出流
            addOut = fs.create(addPath);
            delOut = fs.create(delPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException,
            InterruptedException {
        // 判断是否包含“add”输出到不同文件
        if (key.toString().contains("add")) {
            addOut.write(key.toString().getBytes());
        } else {
            delOut.write(key.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
            InterruptedException {
// 关闭资源
        if (addOut != null) {
            addOut.close();
        }
        if (delOut != null) {
            delOut.close();
        }
    }
}