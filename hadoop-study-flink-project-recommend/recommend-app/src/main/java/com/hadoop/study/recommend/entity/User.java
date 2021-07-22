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
@Document(collection = "users")
public class User {

    @MongoId
    @JsonIgnore
    private String id;

    private Integer userId;

    @Indexed
    private String name;

    private String password;

    @DateTimeFormat(iso = ISO.DATE_TIME)
    private Date createTime;

    public User(String name, String password) {
        this.name = name;
        this.password = password;
    }
}
