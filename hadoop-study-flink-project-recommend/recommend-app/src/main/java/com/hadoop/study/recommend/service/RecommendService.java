package com.hadoop.study.recommend.service;

import com.google.common.collect.Lists;
import com.hadoop.study.recommend.beans.RateProduct;
import com.hadoop.study.recommend.dao.ProductRepository;
import com.hadoop.study.recommend.entity.ProductEntity;
import com.hadoop.study.recommend.hbase.HbaseTemplate;
import com.hadoop.study.recommend.mapper.RateProductMapper;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class RecommendService {

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private HbaseTemplate hbaseTemplate;

    /**
     * 查出所有热门商品清单
     *
     * @param num       数量
     * @param tableName 表名
     * @return {@link ProductEntity}
     * @throws IOException
     */
    public List<ProductEntity> getHistoryHotOrGoodProducts(int num, String tableName) throws IOException {
        List<ProductEntity> recommendEntitys = Lists.newArrayList();
        List<RateProduct> list = Lists.newArrayList();
        for (String productId : hbaseTemplate.getAllKey(tableName)) {
            RateProduct product = hbaseTemplate.get(tableName, productId, new RateProductMapper());
            if (product != null) {
                list.add(product);
            }
        }
        // 排序
        list.sort(Comparator.comparingInt(RateProduct::getCount));

        for (int i = 1; i <= num; i++) {
            ProductEntity product = findProduct(list.get(i).getProductId());
            product.setScore(3.5);
            log.info(product.toString());
            recommendEntitys.add(product);
        }
        return recommendEntitys;
    }

    public ProductEntity findProduct(int productId) {
        return productRepository.findByProductId(productId);
    }

    /**
     * 查询hbase获取itemCFRecommend表对应内容
     *
     * @param productId 产品ID
     * @param tableName 表名
     * @return {@link ProductEntity}
     * @throws IOException
     */
    public List<ProductEntity> getItemCFProducts(int productId, String tableName) throws IOException {
        List<ProductEntity> result = Lists.newArrayList();
        List<Map.Entry> allProducts = HbaseClient.getRow(tableName, String.valueOf(productId));
        RateProduct product = hbaseTemplate.get(tableName, String.valueOf(productId), new RateProductMapper());
        if (allProducts != null) {
            for (Map.Entry entry : allProducts) {
                String id = (String) entry.getKey();
                double sim = (double) entry.getValue();
                ProductEntity product = findProduct(Integer.parseInt(id));
                product.setScore(sim);
                result.add(product);
            }
        }
        return result;
    }

    public List<ProductEntity> getProductByName(String name) {
        return productRepository.likeByName(name);
    }

    public List<ProductEntity> getOnlineRecs(String userId, String tableName) throws IOException {
        List<Map.Entry> allProducts = HbaseClient.getRow(tableName, userId);
        List<ProductEntity> recommends = Lists.newArrayList();

        for (Map.Entry entry : allProducts) {
            String productId = (String) entry.getKey();
            ProductEntity productEntity = productRepository.findByProductId(Integer.parseInt(productId));
            productEntity.setScore(3.5);
            recommends.add(productEntity);
            log.info("onlineRecs: " + productEntity);
        }
        return recommends;
    }

    public List<ProductEntity> getOnlineHot(String tableName, int nums) throws IOException {
        List<String> allKeys = hbaseTemplate.getAllKey(tableName);
        List<ProductEntity> recommends = Lists.newArrayList();

        for (int i = 0; i < nums && i < allKeys.size(); i++) {
            List<Map.Entry> row = HbaseClient.getRow(tableName, String.valueOf(i));
            Double productId = null;
            for (Map.Entry entry : row) {
                if (entry.getKey().equals("productId")) {
                    productId = (Double) entry.getValue();
                }
            }
            ProductEntity productEntity = productRepository.findByProductId(productId.intValue());
            productEntity.setScore(i + 1D);
            recommends.add(productEntity);
        }
        return recommends;
    }
}
