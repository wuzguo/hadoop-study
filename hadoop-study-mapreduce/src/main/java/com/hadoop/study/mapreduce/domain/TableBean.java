package com.hadoop.study.mapreduce.domain;

import lombok.Builder;
import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/27 14:42
 */

@Builder
@Data
public class TableBean implements Writable {

    /**
     * 订单id
     */
    private String orderId;

    /**
     * 产品id
     */
    private String pId;

    /**
     * 产品数量
     */
    private Integer count;

    /**
     * 产品名称
     */
    private String pName;

    /**
     * 标记 0 是订单；1 是产品
     */
    private Integer flag;

    public TableBean() {
    }

    public TableBean(String orderId, String pId, Integer count, String pName, Integer flag) {
        this.orderId = orderId;
        this.pId = pId;
        this.count = count;
        this.pName = pName;
        this.flag = flag;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeUTF(pId);
        out.writeInt(count);
        out.writeUTF(pName);
        out.writeInt(flag);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.orderId = input.readUTF();
        this.pId = input.readUTF();
        this.count = input.readInt();
        this.pName = input.readUTF();
        this.flag = input.readInt();
    }

    @Override
    public String toString() {
        return orderId + '\t' + pId + '\t' + pName + '\t' + count;
    }
}
