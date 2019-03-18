package com.lyf.mr04;

import com.lyf.bean.OrderBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author lyf
 * @date 2019/3/18 0018 下午 9:37
 */
public class OrderReduce extends Reducer<OrderBean, NullWritable, NullWritable, OrderBean> {
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
            context.write(NullWritable.get(), key);
    }
}
