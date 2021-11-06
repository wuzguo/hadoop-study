package com.hadoop.study.doris;

import org.apache.doris.flink.cfg.DorisStreamOptions;
import org.apache.doris.flink.datastream.DorisSourceFunction;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;



public class DorisSourceDataStream {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.put("fenodes","FE_IP:8030");
        properties.put("username","root");
        properties.put("password","");
        properties.put("table.identifier","db.table");
        properties.put("doris.read.field","id,code,name");
        properties.put("doris.filter.query","name='doris'");
        DorisStreamOptions  options = new DorisStreamOptions(properties);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.addSource(new DorisSourceFunction(options,new SimpleListDeserializationSchema())).print();
        env.execute("Flink doris test");
    }
}
