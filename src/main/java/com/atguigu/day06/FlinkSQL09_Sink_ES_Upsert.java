package com.atguigu.day06;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class FlinkSQL09_Sink_ES_Upsert {

    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.读取端口数据创建流,转换为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = env.socketTextStream("hadoop102", 7777)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                });

        //3.将流转换为表
        Table table = tableEnv.fromDataStream(sensorDS);

        //4.TableAPI
        Table tableResult = table.groupBy("id").select("id,id.count as ct");
        tableEnv.createTemporaryView("socket", table);
        Table sqlResult = tableEnv.sqlQuery("select id,ts,count(id) as ct from socket group by id,ts");

        //5.将数据写入ES
        tableEnv.connect(new Elasticsearch()
                .version("6")
                .host("hadoop102", 9200, "http")
                .index("flink_sql04")
                .disableFlushOnCheckpoint()
                .bulkFlushMaxActions(1)
                .documentType("_doc"))
                .inUpsertMode()
                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("ct", DataTypes.BIGINT()))
                .createTemporaryTable("EsPath");

        tableEnv.insertInto("EsPath", sqlResult);

        tableEnv.toRetractStream(sqlResult, Row.class).print();

        //6.执行任务
        env.execute();
    }
}
