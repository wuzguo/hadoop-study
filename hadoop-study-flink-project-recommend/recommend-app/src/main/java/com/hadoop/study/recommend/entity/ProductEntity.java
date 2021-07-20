package com.hadoop.study.recommend.entity;

import java.sql.Timestamp;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.codehaus.jackson.annotate.JsonIgnore;


@Entity
@Table(name = "rec_product")
@NoArgsConstructor
@Data
public class ProductEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @JsonIgnore
    private Integer id;

    private Integer productId;

    @Column(length = 64)
    private String name;

    private String imageUrl;

    @Column(length = 32)
    private String categories;

    @Column(length = 64)
    private String tags;

    private Double score;

    private Timestamp createTime;
}
