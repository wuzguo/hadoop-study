package com.hadoop.study.recommend.service;

import com.google.common.collect.Lists;
import com.hadoop.study.recommend.dao.ProductRepository;
import com.hadoop.study.recommend.entity.ProductEntity;
import com.hadoop.study.recommend.util.HbaseClient;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javafx.util.Pair;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class RecommendService {

    @Autowired
    private ProductRepository productRepository;

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
        List<Pair<String, Double>> list = Lists.newArrayList();
        for (String productId : HbaseClient.getAllKey(tableName)) {
            List<Map.Entry> row = HbaseClient.getRow(tableName, productId);
            if (row != null) {
                double count = (double) row.get(0).getValue();
                list.add(new Pair<>(productId, count));
            }
        }
        // 排序
        list.sort((pre, next) -> next.getValue().compareTo(pre.getValue()));

        for (int i = 1; i <= num; i++) {
            ProductEntity product = getProductEntity(Integer.parseInt(list.get(i).getKey()));
            product.setScore(3.5);
            log.info(product.toString());
            recommendEntitys.add(product);
        }
        return recommendEntitys;
    }

    public ProductEntity getProductEntity(int productId) {
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

        if (allProducts != null) {
            for (Map.Entry entry : allProducts) {
                String id = (String) entry.getKey();
                double sim = (double) entry.getValue();
                ProductEntity product = getProductEntity(Integer.parseInt(id));
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
        List<String> allKeys = HbaseClient.getAllKey(tableName);
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
            productEntity.setScore(i + 1);
            recommends.add(productEntity);
        }
        return recommends;
    }
}
