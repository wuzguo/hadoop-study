package com.hadoop.study.mapreduce.domain;

import lombok.Data;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/26 9:27
 */

@Data
public class AgentBean implements Comparable<AgentBean> {

    /**
     * 城市
     */
    private String city;

    /**
     * 总量
     */
    private Integer total;

    public AgentBean(String city, Integer total) {
        this.city = city;
        this.total = total;
    }

    @Override
    public int compareTo(AgentBean agent) {
        return total.compareTo(agent.total);
    }

    @Override
    public String toString() {
        return city + ":" + total;
    }
}
