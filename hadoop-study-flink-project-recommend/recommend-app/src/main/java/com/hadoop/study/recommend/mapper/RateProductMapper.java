package com.hadoop.study.recommend.mapper;

import com.google.common.collect.Lists;
import com.hadoop.study.recommend.beans.RateProduct;
import com.hadoop.study.recommend.hbase.TableMapper;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/21 16:07
 */

public class RateProductMapper implements TableMapper<RateProduct> {

    /**
     * 列族
     */
    private final static byte[] COLUMN_FAMILY = "rate_product".getBytes(StandardCharsets.UTF_8);

    /**
     * ROW
     */
    private final static byte[] ID = "id".getBytes(StandardCharsets.UTF_8);

    /**
     * 产品ID
     */
    private final static byte[] PRODUCT_ID = "productId".getBytes(StandardCharsets.UTF_8);

    /**
     * 数量
     */
    private final static byte[] COUNT = "count".getBytes(StandardCharsets.UTF_8);

    @Override
    public RateProduct mapRow(Result result, int rowNum) throws Exception {
        return RateProduct.builder()
            .id(Bytes.toInt(result.getValue(COLUMN_FAMILY, ID)))
            .productId(Bytes.toInt(result.getValue(COLUMN_FAMILY, PRODUCT_ID)))
            .count(Bytes.toInt(result.getValue(COLUMN_FAMILY, COUNT)))
            .build();
    }

    @Override
    public List<Mutation> mutations(RateProduct product) throws Exception {
        Put put = new Put(Bytes.toBytes(product.getId()));
        put.addColumn(COLUMN_FAMILY, PRODUCT_ID, Bytes.toBytes(product.getProductId()));
        put.addColumn(COLUMN_FAMILY, COUNT, Bytes.toBytes(product.getCount()));
        return Lists.newArrayList(put);
    }
}
