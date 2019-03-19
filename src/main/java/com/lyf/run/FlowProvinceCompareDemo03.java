package com.lyf.run;

import com.lyf.bean.FlowCompareBean;
import com.lyf.bo.ProvinceComparePartitioner;
import com.lyf.mr03.FlowCountCompareMap;
import com.lyf.mr03.FlowCountCompareReduce;
import com.lyf.utils.HadoopDriverUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @author lyf
 * @date 2019/3/18 0018 下午 8:24
 */
public class FlowProvinceCompareDemo03 {

    public static void main(String []args) throws IOException, ClassNotFoundException, InterruptedException {
        HadoopDriverUtil util = new HadoopDriverUtil();
        util.setMap(FlowCountCompareMap.class, FlowCompareBean.class, Text.class);
        util.setReducer(FlowCountCompareReduce.class, Text.class, FlowCompareBean.class);
        Job job = util.getInstance(FlowProvinceCompareDemo03.class,
                "E:\\xianghaizing\\hadoop_hdfs\\input",
                "E:\\xianghaizing\\hadoop_hdfs\\out");
        job.setPartitionerClass(ProvinceComparePartitioner.class);
        job.setNumReduceTasks(10);
        util.close(job);
    }

}
