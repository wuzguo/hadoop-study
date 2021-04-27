package com.hadoop.study.mapreduce.join.reduce;

import com.google.common.collect.Lists;
import com.hadoop.study.mapreduce.domain.TableBean;
import com.hadoop.study.mapreduce.enums.TypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/27 14:50
 */

@Slf4j
public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        // 1准备存储订单的集合
        List<TableBean> orders = Lists.newArrayList();

        // 获取产品名称
        String pName = "";

        // values 都是以产品ID进行聚合的Bean对象
        for (TableBean bean : values) {
            if (bean.getFlag().equals(TypeEnum.ORDER.getCode())) {
                // 这里要自己新New一个对象，否则会重复
                orders.add(TableBean.builder()
                        .orderId(bean.getOrderId())
                        .pId(bean.getPId())
                        .count(bean.getCount())
                        .pName(bean.getPName())
                        .flag(bean.getFlag()).build());
            } else {
                pName = bean.getPName();
            }
        }

        // 3 表的拼接
        for (TableBean bean : orders) {
            bean.setPName(pName);
            // 4 数据写出去
            context.write(bean, NullWritable.get());
        }
    }
}
