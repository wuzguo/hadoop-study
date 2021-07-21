package com.hadoop.study.recommend.beans;

import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/21 17:09
 */

@Data
@ToString
@Accessors(chain = true)
public class Recommendation {

    /**
     * 产品ID
     */
    private Integer productId;

    /**
     * 评分
     */
    private Double Double;
}
