package com.atguigu.day03;

import com.atguigu.day01.Flink01_WordCount_Batch;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class Flink11_Window_Apply {

    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.读取端口数据
        //DataStreamSource<String> input = env.readTextFile("input");
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 7777);

        //3.压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = input.flatMap(new Flink01_WordCount_Batch.MyFlatMapFunc());

        //4.重分区
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneDS.keyBy(0);

        //5.滚动时间开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowDStream = keyedStream.timeWindow(Time.seconds(5));

        //6.计算
        SingleOutputStreamOperator<Integer> sum = windowDStream.apply(new MyWindowFunc());

        //7.打印
        sum.print();

        //8.执行任务
        env.execute();

    }

    public static class MyWindowFunc implements WindowFunction<Tuple2<String, Integer>, Integer, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Integer> out) throws Exception {

            Integer count = 0;
            Iterator<Tuple2<String, Integer>> iterator = input.iterator();
            while (iterator.hasNext()) {
                Tuple2<String, Integer> next = iterator.next();
                count += 1;
            }

            out.collect(count);
        }
    }

}
