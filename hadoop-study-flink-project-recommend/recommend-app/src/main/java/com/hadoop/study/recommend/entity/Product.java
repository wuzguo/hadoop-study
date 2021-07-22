package com.hadoop.study.recommend.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Date;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoId;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.format.annotation.DateTimeFormat.ISO;


@Data
@NoArgsConstructor
@Document(collection = "products")
public class Product {

    @MongoId
    @JsonIgnore
    private String id;

    private Integer productId;

    @Indexed
    private String name;

    private String imageUrl;

    private String categories;

    private String tags;

    private Double score;

    @DateTimeFormat(iso = ISO.DATE_TIME)
    private Date createTime;
}
