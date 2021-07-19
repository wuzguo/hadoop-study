package com.hadoop.study.recommend.dao;

import com.geekbang.recommend.entity.ProductEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import java.util.List;

public interface ProductRepository extends JpaRepository<ProductEntity, Integer>, JpaSpecificationExecutor<ProductEntity> {

    ProductEntity getProductByProductId(Integer productid);

    @Query("select product from ProductEntity product where product.name like CONCAT('%', :name, '%')")
    List<ProductEntity> findByNameLike(@Param("name") String name);
}
