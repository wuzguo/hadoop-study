package com.hadoop.study.fraud.detect;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/12 16:30
 */
@Slf4j
@SpringBootApplication
public class DetectApplication {

  public static void main(String[] args) {
    SpringApplication.run(DetectApplication.class, args);
  }
}
