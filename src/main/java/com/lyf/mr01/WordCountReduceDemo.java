package com.lyf.mr01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author lyf
 * @date 2019/3/17 9:15
 */
public class WordCountReduceDemo extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        //1. 统计单词个数
        int sum = 0;
        for(IntWritable count : values){
            sum += count.get();
        }
        //2. 输出单词总个数
        context.write(key,new IntWritable(sum));
    }
}
