package com.hadoop.study.mapreduce.domain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.WritableComparable;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/27 19:50
 */

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PageAction implements WritableComparable<PageAction> {

    /**
     * 页面ID
     */
    private Integer pageId;

    /**
     * 访问时间
     */
    private String time;

    @Override
    public int compareTo(PageAction action) {
        return this.time.compareTo(action.time);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(pageId);
        output.writeUTF(time);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.pageId = input.readInt();
        this.time = input.readUTF();
    }
}
