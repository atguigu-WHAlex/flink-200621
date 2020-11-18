package com.atguigu.day03;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Flink04_Sink_JDBC {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件中读取数据创建流
        DataStreamSource<String> inputDS = env.readTextFile("sensor");

        //3.将数据写入MySQL
        inputDS.addSink(new JdbcSink());

        //4.执行任务
        env.execute();

    }

    public static class JdbcSink extends RichSinkFunction<String> {

        //声明MySQL相关的属性信息
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "000000");
            preparedStatement = connection.prepareStatement("INSERT INTO sensor(id,temp) VALUES(?,?) ON DUPLICATE KEY UPDATE temp=?");
        }
        @Override
        public void invoke(String value, Context context) throws Exception {
            //分割数据
            String[] fields = value.split(",");
            //给预编译SQL赋值
            preparedStatement.setString(1, fields[0]);
            preparedStatement.setDouble(2, Double.parseDouble(fields[2]));
            preparedStatement.setDouble(3, Double.parseDouble(fields[2]));
            //执行
            preparedStatement.execute();
        }

        @Override
        public void close() throws Exception {
            preparedStatement.close();
            connection.close();
        }
    }

}
