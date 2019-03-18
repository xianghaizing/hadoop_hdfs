package com.lyf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author lyf
 * @date 2019/3/16 20:13
 */
public class HdfsClient {

    private FileSystem fs;

    @Before
    public void init() throws IOException, URISyntaxException, InterruptedException {
        // 1. 获取配置
        Configuration conf = new Configuration();
        // 2. 配置参数
        // conf.set("fs.defaultFS", "hdfs://node01:9000");
        // 3. 获取hdfs文件系统
        // fs = FileSystem.get(conf);
		fs = FileSystem.get(new URI("hdfs://192.168.37.151:9000"), conf, "root");
    }

    @Test
    public void uploadFile() throws IOException {
        // 上传文件
        fs.copyFromLocalFile(new Path("E:\\xianghaizing\\hadoop_hdfs\\pom.xml"), new Path("/pom.xml"));
    }

    @Test
    public void downloadFile() throws IOException {
        // 下载文件
        fs.copyToLocalFile(false, new Path("/pom.xml"), new Path("e:/pom.xml"), true);
    }

    @After
    public void end() throws IOException {
        fs.close();
    }

}
