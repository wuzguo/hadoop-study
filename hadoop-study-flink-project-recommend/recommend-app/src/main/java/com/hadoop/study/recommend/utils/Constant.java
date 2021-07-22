package com.hadoop.study.recommend.utils;

public class Constant {


    public static String MONGODB_DATABASE = "recommender";

    public static String MONGODB_USER_COLLECTION = "users";

    public static String MONGODB_PRODUCT_COLLECTION = "products";

    public static String MONGODB_RATING_COLLECTION = "ratings";

    public static String MONGODB_AVERAGE_PRODUCTS_SCORE_COLLECTION = "average_products";

    public static String MONGODB_PRODUCT_RECS_COLLECTION = "product_recs";

    public static String MONGODB_RATE_PRODUCTS_COLLECTION = "rate_products";

    public static String MONGODB_RATE_PRODUCTS_RECENTLY_COLLECTION = "rate_recently_products";

    public static String MONGODB_STREAM_RECS_COLLECTION = "stream_recs";

    public static String MONGODB_USER_RECS_COLLECTION = "user_recs";

    public static String MONGODB_ITEM_CF_COLLECTION = "item_cf_product_recs";

    public static String MONGODB_CONTENT_COLLECTION = "content_product_recs";

    public static String PRODUCT_RATING_PREFIX = "rating:";

    public static int REDIS_PRODUCT_RATING_QUEUE_SIZE = 40;
}
