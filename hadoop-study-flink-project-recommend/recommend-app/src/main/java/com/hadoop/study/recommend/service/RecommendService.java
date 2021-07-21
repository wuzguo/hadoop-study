package com.hadoop.study.recommend.service;

import com.google.common.collect.Lists;
import com.hadoop.study.recommend.beans.ProductRecs;
import com.hadoop.study.recommend.beans.RateProduct;
import com.hadoop.study.recommend.beans.Recommendation;
import com.hadoop.study.recommend.beans.UserRecs;
import com.hadoop.study.recommend.dao.ProductRepository;
import com.hadoop.study.recommend.entity.ProductEntity;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

@Slf4j
@Service
public class RecommendService {

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private MongoTemplate mongoTemplate;

    /**
     * 查出所有热门商品清单
     *
     * @param num       数量
     * @param tableName 表名
     * @return {@link ProductEntity}
     */
    public List<ProductEntity> getHistoryHotOrGoodProducts(int num, String tableName) {
        List<RateProduct> products = mongoTemplate.findAll(RateProduct.class, tableName);
        if (CollectionUtils.isEmpty(products)) {
            return Lists.newArrayList();
        }

        // 排序
        products.sort(Comparator.comparingInt(RateProduct::getCount));

        List<ProductEntity> recommendEntitys = Lists.newArrayList();
        for (int i = 1; i <= num; i++) {
            ProductEntity product = findProduct(products.get(i).getProductId());
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
        // 创建条件对象
        Criteria criteria = Criteria.where("productId").is(productId);
        // 创建查询对象，然后将条件对象添加到其中
        ProductRecs productRecs = mongoTemplate.findOne(new Query(criteria), ProductRecs.class, tableName);
        if (productRecs != null) {
            productRecs.getRecs().forEach(recommend -> {
                ProductEntity product = findProduct(recommend.getProductId());
                product.setScore(recommend.getDouble());
                result.add(product);
            });
        }
        return result;
    }

    public List<ProductEntity> getProductByName(String name) {
        return productRepository.likeByName(name);
    }

    public List<ProductEntity> getOnlineRecs(String userId, String tableName) throws IOException {

        // 创建条件对象
        Criteria criteria = Criteria.where("userId").is(userId);
        // 创建查询对象，然后将条件对象添加到其中
        UserRecs userRecs = mongoTemplate.findOne(new Query(criteria), UserRecs.class, tableName);
        if (userRecs == null || CollectionUtils.isEmpty(userRecs.getRecs())) {
            return Lists.newArrayList();
        }
        List<ProductEntity> recommends = Lists.newArrayList();
        for (Recommendation entry : userRecs.getRecs()) {
            Integer productId = entry.getProductId();
            ProductEntity productEntity = productRepository.findByProductId(productId);
            productEntity.setScore(3.5);
            recommends.add(productEntity);
            log.info("onlineRecs: " + productEntity);
        }

        return recommends;
    }

    public List<ProductEntity> getOnlineHot(String tableName, int nums) throws IOException {
        List<RateProduct> rateProducts = mongoTemplate.findAll(RateProduct.class, tableName);
        List<ProductEntity> recommends = Lists.newArrayList();

        for (int i = 0; i < nums && i < rateProducts.size(); i++) {
            RateProduct product = rateProducts.get(i);
            ProductEntity productEntity = productRepository.findByProductId(product.getProductId());
            productEntity.setScore(i + 1D);
            recommends.add(productEntity);
        }
        return recommends;
    }
}
