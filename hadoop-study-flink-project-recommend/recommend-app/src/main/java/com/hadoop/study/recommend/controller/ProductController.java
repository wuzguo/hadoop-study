package com.hadoop.study.recommend.controller;


import com.hadoop.study.recommend.entity.Product;
import com.hadoop.study.recommend.service.RecommendService;
import com.hadoop.study.recommend.utils.Constant;
import java.io.IOException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/product")
public class ProductController {

    private static final String ONLINE_HOT = "onlineHot";

    private static final Integer ONLINE_HOT_NUMS = 10;

    @Autowired
    private RecommendService recommendService;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 热门推荐
     */
    @GetMapping(value = "/history/hot")
    @ResponseBody
    public ModelMap historyHotProducts(@RequestParam("num") Integer nums) {
        ModelMap model = new ModelMap();
        try {
            List<Product> recommendations = recommendService
                .listHistoryHotProducts(Constant.MONGODB_RATE_PRODUCTS_RECENTLY_COLLECTION, nums);
            model.addAttribute("success", true);
            model.addAttribute("products", recommendations);
        } catch (Exception e) {
            log.error("error: {}", e.getMessage());
            model.addAttribute("success", false);
            model.addAttribute("msg", e.getMessage());
        }
        return model;
    }

    /**
     * 优质商品推荐
     *
     * @param nums 数量
     * @return {@link ModelMap}
     */
    @GetMapping(value = "/good/products")
    @ResponseBody
    public ModelMap getGoodProducts(@RequestParam("num") Integer nums) {
        ModelMap model = new ModelMap();
        try {
            List<Product> recommendations = recommendService
                .listHistoryHotProducts(Constant.MONGODB_RATE_PRODUCTS_COLLECTION, nums);
            model.addAttribute("success", true);
            model.addAttribute("products", recommendations);
        } catch (Exception e) {
            log.error("error: {}", e.getMessage());
            model.addAttribute("success", false);
            model.addAttribute("msg", e.getMessage());
        }
        return model;
    }

    /**
     * 基于物品的推荐
     *
     * @param productId 产品ID
     * @return {@link ModelMap}
     */
    @GetMapping(value = "/itemcf/{productId}")
    @ResponseBody
    public ModelMap getItemCFProducts(@PathVariable("productId") Integer productId) {
        ModelMap model = new ModelMap();
        try {
            List<Product> recommendatitons = recommendService
                .getItemCFProducts(Constant.MONGODB_ITEM_CF_COLLECTION, productId);
            model.addAttribute("success", true);
            model.addAttribute("products", recommendatitons);
        } catch (IOException e) {
            log.error("error: {}", e.getMessage());
            model.addAttribute("success", false);
            model.addAttribute("msg", e.getMessage());
        }
        return model;
    }

    /**
     * 查询单个商品
     *
     * @param productId 产品ID
     * @return {@link ModelMap}
     */
    @GetMapping(value = "/query/{productId}")
    @ResponseBody
    public ModelMap queryProductInfo(@PathVariable("productId") Integer productId) {
        ModelMap model = new ModelMap();
        try {
            model.addAttribute("success", true);
            model.addAttribute("products", recommendService.findProduct(productId));
        } catch (Exception e) {
            log.error("error: {}", e.getMessage());
            model.addAttribute("success", false);
            model.addAttribute("msg", e.getMessage());
        }
        return model;
    }

    /**
     * 模糊查询商品
     *
     * @param name 名称
     * @return {@link ModelMap}
     */
    @GetMapping(value = "/search")
    @ResponseBody
    public ModelMap queryProductInfo(@RequestParam("name") String name) {
        ModelMap model = new ModelMap();
        try {
            model.addAttribute("success", true);
            model.addAttribute("products", recommendService.getProductByName(name));
        } catch (Exception e) {
            log.error("error: {}", e.getMessage());
            model.addAttribute("success", false);
            model.addAttribute("msg", e.getMessage());
        }
        return model;
    }

    /**
     * 将评分数据发送到 kafka 'rating' Topic
     *
     * @param productId 产品ID
     * @param score     评分
     * @param userId    用户ID
     * @return {@link ModelMap}
     */
    @GetMapping(value = "/rate/{productId}")
    @ResponseBody
    public ModelMap queryProductInfo(@PathVariable("productId") Integer productId,
        @RequestParam("score") Double score, @RequestParam("userId") Integer userId) {
        ModelMap model = new ModelMap();
        try {
            String logger = userId + "," + productId + "," + score + "," + System.currentTimeMillis() / 1000;
            kafkaTemplate.send("recommender", logger);
            log.info(Constant.PRODUCT_RATING_PREFIX + logger);
            model.addAttribute("success", true);
            model.addAttribute("message", "完成评分");
        } catch (Exception e) {
            log.error("error: {}", e.getMessage());
            model.addAttribute("success", false);
            model.addAttribute("msg", e.getMessage());
        }
        return model;
    }


    /**
     * 实时用户个性化推荐
     *
     * @param userId 用户ID
     * @return {@link ModelMap}
     */
    @GetMapping(value = "/stream")
    @ResponseBody
    public ModelMap onlineRecs(@RequestParam("userId") String userId) {
        ModelMap model = new ModelMap();
        try {
            List<Product> res = recommendService.getOnlineRecs(Constant.MONGODB_STREAM_RECS_COLLECTION, userId);
            model.addAttribute("success", true);
            model.addAttribute("products", res);
        } catch (Exception e) {
            log.error("error: {}", e.getMessage());
            model.addAttribute("success", false);
            model.addAttribute("msg", "查询失败");
        }
        return model;
    }

    /**
     * 实时热门推荐
     *
     * @return {@link ModelMap}
     */
    @GetMapping(value = "/online/hot")
    @ResponseBody
    public ModelMap onlineHot() {
        ModelMap model = new ModelMap();
        try {
            List<Product> res = recommendService.getOnlineHot(ONLINE_HOT, ONLINE_HOT_NUMS);
            model.addAttribute("success", true);
            model.addAttribute("products", res);
        } catch (Exception e) {
            log.error("error: {}", e.getMessage());
            model.addAttribute("success", false);
            model.addAttribute("msg", e.getMessage());
        }
        return model;
    }
}
