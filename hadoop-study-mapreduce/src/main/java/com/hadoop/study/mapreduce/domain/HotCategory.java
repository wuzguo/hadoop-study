package com.hadoop.study.mapreduce.domain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/27 14:07
 */

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class HotCategory implements Writable {

    /**
     * 品类ID
     */
    private Long categoryId;

    /**
     * 点击总数
     */
    private Integer clickCount;

    /**
     * 下单总数
     */
    private Integer orderCount;

    /**
     * 支付总数
     */
    private Integer payCount;


    /**
     * 添加
     *
     * @param category {@link HotCategory}
     * @return {@link HotCategory}
     */
    public HotCategory add(HotCategory category) {
        int clickCount = category.getClickCount() + this.clickCount;
        int orderCount = category.getOrderCount() + this.orderCount;
        int payCount = category.getPayCount() + this.payCount;
        return HotCategory.builder().categoryId(category.categoryId).clickCount(clickCount).orderCount(orderCount)
            .payCount(payCount).build();
    }


    @Override
    public String toString() {
        return categoryId + ": " + clickCount + "," + orderCount + "," + payCount;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeLong(categoryId);
        output.writeInt(clickCount);
        output.writeInt(orderCount);
        output.writeInt(payCount);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.categoryId = input.readLong();
        this.clickCount = input.readInt();
        this.orderCount = input.readInt();
        this.payCount = input.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HotCategory category = (HotCategory) o;
        return Objects.equals(categoryId, category.categoryId) && Objects
            .equals(clickCount, category.clickCount) && Objects.equals(orderCount, category.orderCount)
            && Objects.equals(payCount, category.payCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(categoryId, clickCount, orderCount, payCount);
    }
}
