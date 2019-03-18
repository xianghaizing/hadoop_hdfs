package com.lyf.mr01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author lyf
 * @date 2019/3/17 8:59
 * KEYIN, VALUEIN, KEYOUT, VALUEOUT
 */
public class WordCountMapDemo extends Mapper<LongWritable, Text, Text, IntWritable>{

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //1.文本转换为字符串
        String line = value.toString();
        //2.切割
        String[] words = line.split(" ");
        //3.循环统计
        for (String word : words) {
            //4. 写出到下一阶段
            /***
             * 此时以一个单词为key, 计数器+1
             * */
            context.write(new Text(word), new IntWritable(1));
        }
    }


}
