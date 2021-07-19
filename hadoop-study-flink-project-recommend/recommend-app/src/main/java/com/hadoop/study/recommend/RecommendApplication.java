package com.geekbang.recommend;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
@EnableWebSecurity
public class RecommendBackendApplication {
    public static void main(String[] args) {
        SpringApplication.run(RecommendBackendApplication.class, args);
    }
}
