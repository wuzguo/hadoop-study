package com.hadoop.study.mapreduce.agent;

import com.google.common.collect.Maps;
import com.hadoop.study.mapreduce.domain.AgentBean;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/26 9:19
 */

@Slf4j
public class OneAgentReducer extends Reducer<Text, Text, Text, Text> {

    private final Text value = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 设置
        Map<String, AgentBean> mapAgents = Maps.newHashMap();
        // 遍历
        values.forEach(text -> {
            AgentBean agent = mapAgents.getOrDefault(text.toString(), new AgentBean(text.toString(), 0));
            agent.setTotal(Optional.ofNullable(agent.getTotal()).orElse(0) + 1);
            mapAgents.put(text.toString(), agent);
        });

        // 组装字符串
        String citys = mapAgents.values().stream().sorted(Comparator.reverseOrder()).limit(3)
            .map(AgentBean::toString).collect(Collectors.joining("， "));
        value.set(citys);
        context.write(key, value);
    }
}
