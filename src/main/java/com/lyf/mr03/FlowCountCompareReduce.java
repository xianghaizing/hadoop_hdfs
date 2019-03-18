package com.lyf.mr03;


import com.lyf.bean.FlowCompareBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountCompareReduce extends Reducer<FlowCompareBean, Text, Text, FlowCompareBean> {
    @Override
    protected void reduce(FlowCompareBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text text : values){
            context.write(text, key);
        }
    }
}