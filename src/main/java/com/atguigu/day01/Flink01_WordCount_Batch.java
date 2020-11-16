package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Flink01_WordCount_Batch {

    public static void main(String[] args) throws Exception {

        //1.创建Flink程序的入口
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.读取文件数据 "hello atguigu"
        DataSource<String> lineDS = env.readTextFile("input");

        //3.压平操作 "hello atguigu" => (hello 1),(atguigu 1)
        FlatMapOperator<String, Tuple2<String, Integer>> wordToOneDS = lineDS.flatMap(new MyFlatMapFunc());

        //4.分组
        UnsortedGrouping<Tuple2<String, Integer>> groupByDS = wordToOneDS.groupBy(0);

        //5.聚合计算
        AggregateOperator<Tuple2<String, Integer>> result = groupByDS.sum(1);

        //6.打印结果
        result.print();

    }

    //自定义的FlatMapFunction
    public static class MyFlatMapFunc implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            //按照空格切分value
            String[] words = value.split(" ");
            //遍历words输出数据
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }

}
