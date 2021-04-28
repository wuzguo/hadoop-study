package com.hadoop.study.mapreduce.domain;

import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/28 16:41
 */

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DisorderPair implements WritableComparable<DisorderPair> {

    private Set<String> values;

    @Override
    public int compareTo(DisorderPair o) {
        if (values.containsAll(o.values)) {
            return 0;
        } else {
            List<String> srcValues = values.stream().sorted(String::compareTo).collect(Collectors.toList());
            List<String> destValues = o.getValues().stream().sorted(String::compareTo).collect(Collectors.toList());
            int val = srcValues.get(0).compareTo(destValues.get(0));
            if (val == 0) {
                return srcValues.get(1).compareTo(destValues.get(1));
            }
            return val;
        }
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeUTF(String.join("\t", values));
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.values = Sets.newHashSet(input.readUTF().split("\t"));
    }
}
