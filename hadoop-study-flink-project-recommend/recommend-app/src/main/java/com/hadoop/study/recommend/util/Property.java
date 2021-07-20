package com.hadoop.study.recommend.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Property {

    private static Properties properties;

    private final static String CONFIG_NAME = "config.properties";

    static {
        InputStream config = Thread.currentThread().getContextClassLoader().getResourceAsStream(CONFIG_NAME);
        System.setProperty("hadoop.home.dir", "D:\\app\\hadoop\\hadoop-2.9.2");
        properties = new Properties();
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(config, StandardCharsets.UTF_8);
            properties.load(inputStreamReader);
        } catch (IOException e) {
            log.error("配置文件加载失败");
            e.printStackTrace();
        }
    }

    public static String getStrValue(String key) {
        return properties.getProperty(key);
    }

    public static Integer getIntegerValue(String key) {
        return Integer.parseInt(getStrValue(key));
    }

    public static Properties getKafkaProperties(String groupId) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", Property.getStrValue("kafka.bootstrap.servers"));
        properties.setProperty("zookeeper.connect", Property.getStrValue("kafka.zookeeper.connect"));
        properties.setProperty("group.id", groupId);
        return properties;
    }
}
