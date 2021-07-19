package com.hadoop.study.recommend.dao;


import com.hadoop.study.recommend.entity.ProductEntity;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;


public interface ProductRepository extends JpaRepository<ProductEntity, Integer>,
    JpaSpecificationExecutor<ProductEntity> {

    ProductEntity findByProductId(Integer productId);

    @Query("select product from ProductEntity product where product.name like CONCAT('%', :name, '%')")
    List<ProductEntity> likeByName(@Param("name") String name);
}
