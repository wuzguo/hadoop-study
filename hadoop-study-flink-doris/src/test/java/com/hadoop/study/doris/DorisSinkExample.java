package com.hadoop.study.doris;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

public class DorisSinkExample {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        List<Tuple2<String, Integer>> data = new ArrayList<>();
        data.add(new Tuple2<>("doris",1));
        DataStreamSource<Tuple2<String, Integer>> source = env.fromCollection(data);
        tEnv.createTemporaryView("doris_test",source,$("name"),$("age"));

        tEnv.executeSql(
                "CREATE TABLE doris_test_sink (" +
                        "name STRING," +
                        "age INT" +
                        ") " +
                        "WITH (\n" +
                        "  'connector' = 'doris',\n" +
                        "  'fenodes' = 'FE_IP:8030',\n" +
                        "  'table.identifier' = 'db.table',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = '',\n" +
                        "  'sink.properties.format' = 'json',\n" +
                        "  'sink.properties.strip_outer_array' = 'true'\n" +
                        ")");

        tEnv.executeSql("INSERT INTO doris_test_sink select name,age from doris_test");
    }
}
