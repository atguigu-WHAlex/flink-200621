package com.atguigu.day02;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink07_Transform_Filter {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件读取数据
        DataStreamSource<String> fileDS = env.readTextFile("sensor");

        //3.过滤,取出30度以上的数据
        SingleOutputStreamOperator<String> filter = fileDS.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                double temp = Double.parseDouble(value.split(",")[2]);
                return temp > 30.0D;
            }
        });


        filter.print();
        env.execute();

    }
}
