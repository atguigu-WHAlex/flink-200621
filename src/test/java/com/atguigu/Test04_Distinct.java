package com.atguigu;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class Test04_Distinct {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件中读取数据创建流
        DataStreamSource<String> inputDS = env.readTextFile("input");

        //3.压平
        SingleOutputStreamOperator<String> wordDS = inputDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        //4.使用hashSet方式去重
        SingleOutputStreamOperator<String> filter = wordDS.filter(new FilterFunction<String>() {

            HashSet<String> words = new HashSet<>();

            @Override
            public boolean filter(String value) throws Exception {
                boolean exist = words.contains(value);
                if (!exist) {
                    words.add(value);
                }
                return !exist;
            }
        });

        //5.打印数据
        filter.print();

        //6.执行任务
        env.execute();

    }
}
