package com.hadoop.study.mapreduce.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/27 16:30
 */

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
public class LogBean {

    /**
     * 记录客户端的ip地址
     */
    private String remoteAddr;

    /**
     * 记录客户端用户名称,忽略属性
     */
    private String remoteUser;

    /**
     * 记录访问时间与时区
     */
    private String timeLocal;

    /**
     * 记录请求的url与http协议
     */
    private String request;

    /**
     * 记录请求状态；成功是200
     */
    private String status;

    /**
     * 记录发送给客户端文件主体内容大小
     */
    private String bodyBytesSent;

    /**
     * 用来记录从那个页面链接访问过来的
     */
    private String httpReferer;

    /**
     * 记录客户浏览器的相关信息
     */
    private String httpUserAgent;

    /**
     * 判断数据是否合法
     */
    private Boolean valid;

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(remoteAddr).append('\t');
        builder.append(remoteUser).append('\t');
        builder.append(timeLocal).append('\t');
        builder.append(request).append('\t');
        builder.append(status).append('\t');
        builder.append(bodyBytesSent).append('\t');
        builder.append(httpReferer).append('\t');
        builder.append(httpUserAgent).append('\t');
        return builder.toString();
    }
}
