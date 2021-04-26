package com.hadoop.study.mapreduce.domain;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
public class OrderBean implements WritableComparable<OrderBean> {
    /**
     * 订单id号
     */
    private Integer orderId;

    /**
     * 订单金额
     */
    private Double amount;

    public OrderBean() {
    }

    public OrderBean(Integer orderId, Double amount) {
        this.orderId = orderId;
        this.amount = amount;
    }

    @Override
    public int compareTo(OrderBean order) {
        if (getOrderId() > order.getOrderId()) {
            return 1;
        } else if (getOrderId() < order.getOrderId()) {
            return -1;
        } else {
            // 价格倒序排序，如果这里 return 0，那么就取到各个订单的最小值
            return amount > order.getAmount() ? -1 : 1;
        }
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(orderId);
        output.writeDouble(amount);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.orderId = input.readInt();
        this.amount = input.readDouble();
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "orderId='" + orderId + '\'' +
                ", amount=" + amount +
                '}';
    }
}
