package com.lyf.run;

import com.lyf.bean.FlowBean;
import com.lyf.mr02.FlowCountMap;
import com.lyf.mr02.FlowCountReduce;
import com.lyf.utils.HadoopDriverUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @author lyf
 * @date 2019/3/18 0018 下午 8:24
 */
public class FlowDemo02 {

    public static void main(String []args) throws IOException, ClassNotFoundException, InterruptedException {
        HadoopDriverUtil util = new HadoopDriverUtil();
        util.setMap(FlowCountMap.class, Text.class, FlowBean.class);
        util.setReducer(FlowCountReduce.class, Text.class, FlowBean.class);
        Job job = util.getInstance(FlowDemo02.class,
                "E:\\xianghaizing\\hadoop_hdfs\\input",
                "E:\\xianghaizing\\hadoop_hdfs\\out");
        util.close(job);
    }

}
