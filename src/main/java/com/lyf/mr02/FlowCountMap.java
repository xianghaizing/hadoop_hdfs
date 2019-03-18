package com.lyf.mr02;

import com.lyf.bean.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMap extends Mapper<LongWritable, Text, Text, FlowBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // 1. 获取值并切分成数组
        String line = value.toString();
        String[] fields = line.split("\t");

        // 2. 获取手机号
        String phoneNum = fields[1];
        Long upFlow = Long.valueOf(fields[5]);
        Long downFlow = Long.valueOf(fields[6]);

        // 3. 写出
        context.write(new Text(phoneNum), new FlowBean(upFlow, downFlow));
    }
}