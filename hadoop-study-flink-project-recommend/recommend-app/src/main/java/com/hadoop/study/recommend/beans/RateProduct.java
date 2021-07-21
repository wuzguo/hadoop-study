package com.hadoop.study.recommend.beans;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/21 16:06
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class RateProduct {

    /**
     * 消息ID
     */
    private Integer id;

    /**
     * 产品ID
     */
    private Integer productId;

    /**
     * 数量
     */
    private Integer count;
}
