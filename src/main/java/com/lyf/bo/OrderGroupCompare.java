package com.lyf.bo;

import com.lyf.bean.OrderBean;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author lyf
 * @date 2019/3/18 0018 下午 9:22
 */
public class OrderGroupCompare extends WritableComparator {

    protected OrderGroupCompare(){
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;
        return aBean.getOrderId()-bBean.getOrderId();
    }
}
