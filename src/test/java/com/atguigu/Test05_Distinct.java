package com.atguigu;

import com.atguigu.day01.Flink01_WordCount_Batch;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test05_Distinct {

    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从端口获取数据创建流
        DataStreamSource<String> lineDS = env.socketTextStream("hadoop102", 7777);

        //3.压平操作,并给算子局部设置并行度
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = lineDS.flatMap(new Flink01_WordCount_Batch.MyFlatMapFunc());

        //4.分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedDS = wordToOneDS.keyBy(0);

        //5.计算聚合结果
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToCount = keyedDS.sum(1);

        //6.过滤,只取count==1的数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> filter = wordToCount.filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> value) throws Exception {
                return value.f1 == 1;
            }
        });

        //7.打印结果
        filter.map(new MapFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String map(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).print();

        //8.执行任务
        env.execute();

    }

}
