package com.hadoop.study.doris;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class DorisSourceSinkExample {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        tEnv.executeSql(
                "CREATE TABLE doris_test (" +
                        "name STRING," +
                        "age INT," +
                        "price DECIMAL(5,2)," +
                        "sale DOUBLE" +
                        ") " +
                        "WITH (\n" +
                        "  'connector' = 'doris',\n" +
                        "  'fenodes' = 'FE_IP:8030',\n" +
                        "  'table.identifier' = 'db.table',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = ''" +
                        ")");
        tEnv.executeSql(
                "CREATE TABLE doris_test_sink (" +
                        "name STRING," +
                        "age INT," +
                        "price DECIMAL(5,2)," +
                        "sale DOUBLE" +
                        ") " +
                        "WITH (\n" +
                        "  'connector' = 'doris',\n" +
                        "  'fenodes' = 'FE_IP:8030',\n" +
                        "  'table.identifier' = 'db.table',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = '',\n" +
                        "  'sink.batch.size' = '3',\n" +
                        "  'sink.max-retries' = '2'\n" +
                        ")");

        tEnv.executeSql("INSERT INTO doris_test_sink select name,age,price,sale from doris_test");
    }
}
