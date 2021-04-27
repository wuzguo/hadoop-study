package com.hadoop.study.mapreduce.join.map;

import com.google.common.collect.Maps;
import com.hadoop.study.mapreduce.domain.TableBean;
import com.hadoop.study.mapreduce.enums.TypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/27 15:42
 */

@Slf4j
public class TableCacheMapper extends Mapper<LongWritable, Text, TableBean, NullWritable> {

    // 产品ID和产品名称
    private Map<String, String> mapName = Maps.newHashMap();

    // 文件名称
    private String fileName = "";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 1 获取缓存的文件
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath();
        log.info("cache file name: {}", path);
        // 读取数据
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8))) {
            String line;
            while (StringUtils.isNotEmpty(line = reader.readLine())) {
                // 2 切割
                String[] fields = line.split("\t");
                // 3 缓存数据到集合
                mapName.put(fields[0], fields[1]);
            }
        }

        // 获取当前读取的文件名称
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        // 如果是订单的才处理
        if (fileName.startsWith(TypeEnum.ORDER.getValue())) {
            TableBean order = TableBean.builder().orderId(fields[0])
                    .flag(TypeEnum.ORDER.getCode())
                    .pName(mapName.getOrDefault(fields[1], ""))
                    .count(Integer.valueOf(fields[2]))
                    .pId(fields[1]).build();
            context.write(order, NullWritable.get());
        }
    }
}
