package com.lyf.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author lyf
 * @date 2019/3/17 12:11
 */
public class FlowBean implements Writable{

    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public FlowBean() {
        super();
    }

    public FlowBean(Long upFlow, Long downFlow) {
        this.upFlow  = upFlow;
        this.downFlow  = downFlow;
        this.sumFlow = upFlow + downFlow;
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

    public int compareTo(FlowBean o){
        // 倒序排列,从大到小
        return this.sumFlow > o.sumFlow ? -1 : 1;
    }

    @Override
    public String toString() {
        return "\t" + upFlow + "\t\t" + downFlow + "\t\t" + sumFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }
}
