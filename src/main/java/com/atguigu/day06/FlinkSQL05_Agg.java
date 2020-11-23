package com.atguigu.day06;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSQL05_Agg {

    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取文本数据创建流并转换为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = env.readTextFile("sensor/sensor.txt")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                });

        //3.创建TableAPI FlinkSQL 的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //4.使用Table执行环境将流转换为Table
        tableEnv.createTemporaryView("sensor", sensorDS);

        //5.TableAPI
        Table table = tableEnv.from("sensor");
        Table tableResult = table.groupBy("id").select("id,id.count,temp.avg");

        //6.SQL
        Table sqlResult = tableEnv.sqlQuery("select id,count(id) from sensor group by id");

        //7.将结果数据打印
        tableEnv.toRetractStream(tableResult, Row.class).print("tableResult");
        tableEnv.toRetractStream(sqlResult, Row.class).print("sqlResult");

        //8.执行
        env.execute();

    }

}
