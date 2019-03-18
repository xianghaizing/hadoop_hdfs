package com.lyf.mr02;

import com.lyf.bean.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReduce extends Reducer<Text, FlowBean, Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context)
            throws IOException, InterruptedException {

        long sum_upFlow = 0;
        long sum_downFlow = 0;
        for (FlowBean bean : values) {
            sum_upFlow += bean.getUpFlow();
            sum_downFlow += bean.getDownFlow();
        }
        FlowBean flowBean = new FlowBean(sum_upFlow, sum_downFlow);
        context.write(key, flowBean);
    }
}