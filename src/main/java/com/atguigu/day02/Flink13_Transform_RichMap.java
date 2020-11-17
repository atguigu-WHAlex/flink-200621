package com.atguigu.day02;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink13_Transform_RichMap {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件读取数据
        DataStreamSource<String> fileDS = env.readTextFile("sensor");

        //3.将每一行数据转换为JavaBean对象
        SingleOutputStreamOperator<SensorReading> map = fileDS.map(new MyRichMapFunc());

        //4.打印数据
        map.print();

        //5.执行
        env.execute();

    }

    public static class MyRichMapFunc extends RichMapFunction<String, SensorReading> {


        @Override
        public void open(Configuration parameters) throws Exception {
            //创建连接
            super.open(parameters);
            System.out.println("open方法被调用");
        }

        @Override
        public SensorReading map(String value) throws Exception {
            //使用连接
            String[] fields = value.split(",");
            return new SensorReading(fields[0],
                    Long.parseLong(fields[1]),
                    Double.parseDouble(fields[2]));
        }

        @Override
        public void close() throws Exception {
            //关闭连接
            super.close();
            System.out.println("close方法被调用");
        }
    }


}
