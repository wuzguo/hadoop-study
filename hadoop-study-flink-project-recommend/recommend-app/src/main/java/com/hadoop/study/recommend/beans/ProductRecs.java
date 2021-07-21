package com.hadoop.study.recommend.beans;

import java.util.List;
import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.springframework.data.mongodb.core.mapping.MongoId;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/21 17:08
 */

@Data
@ToString
@Accessors(chain = true)
public class ProductRecs {

    /**
     * 消息ID
     */
    @MongoId
    private String id;

    /**
     * 产品ID
     */
    private Integer productId;

    /**
     * 相似度列表
     */
    private List<Recommendation> recs;
}
