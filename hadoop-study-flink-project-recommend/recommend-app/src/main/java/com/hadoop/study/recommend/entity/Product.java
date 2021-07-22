package com.hadoop.study.recommend.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.sql.Timestamp;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;


@Data
@NoArgsConstructor
@Document(collection = "products")
public class Product {

    @Id
    @JsonIgnore
    private Integer id;

    private Integer productId;

    @Indexed
    private String name;

    private String imageUrl;

    private String categories;

    private String tags;

    private Double score;

    private Timestamp createTime;
}
