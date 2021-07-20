package com.hadoop.study.spark.recomnend.beans

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/20 14:33
 */

/**
 * Product数据集
 * 3982                            商品ID
 * Fuhlen 富勒 M8眩光舞者时尚节能    商品名称
 * 1057,439,736                    商品分类ID，不需要
 * B009EJN4T2                      亚马逊ID，不需要
 * https://images-cn-4.ssl-image   商品的图片URL
 * 外设产品|鼠标|电脑/办公           商品分类
 * 富勒|鼠标|电子产品|好用|外观漂亮   商品UGC标签
 */
case class Product(productId: Int, name: String, imageUrl: String, categories: String, tags: String, createTime: Long)
