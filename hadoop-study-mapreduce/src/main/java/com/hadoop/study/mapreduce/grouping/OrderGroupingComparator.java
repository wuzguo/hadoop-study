package com.hadoop.study.mapreduce.grouping;

import com.hadoop.study.mapreduce.domain.OrderBean;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator {

    protected OrderGroupingComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;
        if (aBean.getOrderId() > bBean.getOrderId()) {
            return 1;
        } else if (aBean.getOrderId() < bBean.getOrderId()) {
            return -1;
        } else {
            return 0;
        }
    }
}
