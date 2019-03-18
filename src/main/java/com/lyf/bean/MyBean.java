package com.lyf.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author lyf
 * @date 2019/3/17 12:11
 */
public class MyBean implements Writable{

    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public MyBean() {
        super();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    public int compareTo(MyBean o){
        // 倒序排列,从大到小
        return this.sumFlow > o.sumFlow ? -1 : 1;
    }

    @Override
    public String toString() {
        return "MyBean{" +
                "upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", sumFlow=" + sumFlow +
                '}';
    }


}
