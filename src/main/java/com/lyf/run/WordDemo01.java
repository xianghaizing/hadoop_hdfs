package com.lyf.run;

import com.lyf.mr01.WordCountMapDemo;
import com.lyf.mr01.WordCountReduceDemo;
import com.lyf.utils.HadoopDriverUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @author lyf
 * @date 2019/3/18 0018 下午 8:24
 */
public class WordDemo01 {

    public static void main(String []args) throws IOException, ClassNotFoundException, InterruptedException {
        HadoopDriverUtil util = new HadoopDriverUtil();
        util.setMap(WordCountMapDemo.class, Text.class, IntWritable.class);
        util.setReducer(WordCountReduceDemo.class, Text.class, IntWritable.class);
        Job job = util.getInstance(WordDemo01.class,
                "E:\\xianghaizing\\hadoop_hdfs\\input",
                "E:\\xianghaizing\\hadoop_hdfs\\out");
        util.close(job);
    }

}
