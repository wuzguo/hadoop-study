package com.hadoop.study.doris;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class DorisSourceExample {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // source only supports parallelism of 1

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // register a table in the catalog
        tEnv.executeSql(
                "CREATE TABLE doris_source (" +
                        "bigint_1 BIGINT," +
                        "char_1 STRING," +
                        "date_1 STRING," +
                        "datetime_1 STRING," +
                        "decimal_1 DECIMAL(5,2)," +
                        "double_1 DOUBLE," +
                        "float_1 FLOAT ," +
                        "int_1 INT ," +
                        "largeint_1 STRING, " +
                        "smallint_1 SMALLINT, " +
                        "tinyint_1 TINYINT, " +
                        "varchar_1 STRING " +
                        ") " +
                        "WITH (\n" +
                        "  'connector' = 'doris',\n" +
                        "  'fenodes' = 'FE_IP:8030',\n" +
                        "  'table.identifier' = 'db.table',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = ''\n" +
                        ")");

        // define a dynamic aggregating query
        final Table result = tEnv.sqlQuery("SELECT * from doris_source  ");

        // print the result to the console
        tEnv.toRetractStream(result, Row.class).print();
        env.execute();
    }
}
