package com.atguigu.day01;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 设置并行度参数优先级:
 * 1.代码优先级最高:
 * 1.1局部设置并行度
 * 1.2.全局设置并行度
 * <p>
 * 2.提交任务时的命令行
 * <p>
 * 3.默认配置文件
 */
public class Flink03_WordCount_Unbounded {

    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //代码全局设置并行度
        //        env.setParallelism(2);
        env.disableOperatorChaining();

        //提取参数
//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        String host = parameterTool.get("host");
//        int port = parameterTool.getInt("port");

        //2.从端口获取数据创建流
        DataStreamSource<String> lineDS = env.socketTextStream("hadoop102", 7777);

        //3.压平操作,并给算子局部设置并行度
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = lineDS.flatMap(new Flink01_WordCount_Batch.MyFlatMapFunc());

        //4.分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedDS = wordToOneDS.keyBy(0);

        //5.计算聚合结果
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedDS.sum(1);

        //6.打印数据结果
        result.print();

        //7.启动任务
        env.execute("Flink03_WordCount_Unbounded");

    }

}
