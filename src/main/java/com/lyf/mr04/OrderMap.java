package com.lyf.mr04;

import com.lyf.bean.OrderBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author lyf
 * @date 2019/3/18 0018 下午 9:21
 */
public class OrderMap extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        OrderBean order = new OrderBean(Integer.parseInt(words[0]),words[1],Double.valueOf(words[2]));
        System.out.println(order);
        context.write(order, NullWritable.get());
    }
}
