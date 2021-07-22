package com.hadoop.study.recommend.service;

import com.google.common.collect.Lists;
import com.hadoop.study.recommend.beans.ProductRecs;
import com.hadoop.study.recommend.beans.RateProduct;
import com.hadoop.study.recommend.beans.Recommendation;
import com.hadoop.study.recommend.beans.UserRecs;
import com.hadoop.study.recommend.entity.Product;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
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
    private MongoTemplate mongoTemplate;

    /**
     * 查出所有热门商品清单
     *
     * @param nums      数量
     * @param tableName 表名
     * @return {@link Product}
     */
    public List<Product> listHistoryHotProducts(String tableName, Integer nums) {
        return Optional.of(mongoTemplate.findAll(RateProduct.class, tableName))
            .orElse(Lists.newArrayList()).stream()
            .sorted(Comparator.comparingInt(RateProduct::getCount)).limit(nums)
            .map(product -> {
                Product productEntity = findProduct(product.getProductId());
                productEntity.setScore(3.5);
                log.info(productEntity.toString());
                return productEntity;
            }).collect(Collectors.toList());
    }

    public Product findProduct(Integer productId) {
        Criteria criteria = Criteria.where("productId").is(productId);
        return mongoTemplate.findOne(new Query(criteria), Product.class);
    }

    /**
     * 查询hbase获取itemCFRecommend表对应内容
     *
     * @param productId 产品ID
     * @param tableName 表名
     * @return {@link Product}
     * @throws IOException
     */
    public List<Product> getItemCFProducts(String tableName, Integer productId) throws IOException {
        List<Product> productEntities = Lists.newArrayList();
        // 创建条件对象
        Criteria criteria = Criteria.where("productId").is(productId);
        // 创建查询对象，然后将条件对象添加到其中
        ProductRecs productRecs = mongoTemplate.findOne(new Query(criteria), ProductRecs.class, tableName);
        if (productRecs != null) {
            productRecs.getRecs().forEach(recommend -> {
                Product product = findProduct(recommend.getProductId());
                product.setScore(recommend.getDouble());
                productEntities.add(product);
            });
        }
        return productEntities;
    }

    public List<Product> getProductByName(String name) {
        Criteria criteria = Criteria.where("name").regex(String.format(".*%s.*", name), "i");
        return mongoTemplate.find(new Query(criteria), Product.class);
    }

    public List<Product> getOnlineRecs(String tableName, String userId) {
        // 创建查询对象，然后将条件对象添加到其中
        UserRecs userRecs = mongoTemplate
            .findOne(new Query(Criteria.where("userId").is(userId)), UserRecs.class, tableName);
        if (userRecs == null || CollectionUtils.isEmpty(userRecs.getRecs())) {
            return Lists.newArrayList();
        }
        List<Product> products = Lists.newArrayList();
        for (Recommendation recommendation : userRecs.getRecs()) {
            Integer productId = recommendation.getProductId();
            Product product = mongoTemplate
                .findOne(new Query(Criteria.where("productId").is(productId)), Product.class);
            product.setScore(3.5);
            products.add(product);
            log.info("onlineRecs: " + product);
        }
        return products;
    }

    public List<Product> getOnlineHot(String tableName, Integer nums) {
        List<RateProduct> rateProducts = Optional.of(mongoTemplate.findAll(RateProduct.class, tableName))
            .orElse(Lists.newArrayList());
        List<Product> recommends = Lists.newArrayList();
        for (int i = 0; i < nums && i < rateProducts.size(); i++) {
            RateProduct rateProduct = rateProducts.get(i);
            Product product = mongoTemplate
                .findOne(new Query(Criteria.where("productId").is(rateProduct.getProductId())), Product.class);
            product.setScore(i + 1D);
            recommends.add(product);
        }
        return recommends;
    }
}
