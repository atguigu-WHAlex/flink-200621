package com.atguigu.day07;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSQL05_ProcessTime_Session {

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
        Table table = tableEnv.fromDataStream(sensorDS, "id,ts,temp,pt.proctime");

        //4.会话窗口
//        Table result = table.window(Session.withGap("5.seconds").on("pt").as("sw"))
//                .groupBy("id,sw")
//                .select("id,id.count");

        //SQL
        tableEnv.createTemporaryView("sensor", table);
        Table result = tableEnv.sqlQuery("select id,count(id) as ct from sensor " +
                "group by id,session(pt,INTERVAL '5' second)");

        //5.转换为流进行输出
        tableEnv.toAppendStream(result, Row.class).print();

        //6.执行
        env.execute();

    }

}
