package com.atguigu;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

public class Test06_Window_Watermark {

    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置时间语义为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取端口数据创建流
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        //3.指定数据中的时间字段
        SingleOutputStreamOperator<SensorReading> sensorDataStream = socketTextStream
                .map(line -> {
                    String[] split = line.split(",");
                    return new SensorReading(split[0], Long.parseLong(split[1]), 1.0);
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        return element.getTs() * 1000L;
                    }
                });

        //4.分组
        KeyedStream<SensorReading, Tuple> keyedStream = sensorDataStream.keyBy("id");

        //5.滑动窗口 30秒的窗口大小,5秒的滑动步长
        WindowedStream<SensorReading, Tuple, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(30), Time.seconds(5))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<SensorReading>("sideOutPut") {
                });

        //6.计算WordCount
        SingleOutputStreamOperator<SensorReading> result = windowedStream.sum("temp");

        //7.提取侧输出流数据
        DataStream<SensorReading> sideOutput = result.getSideOutput(new OutputTag<SensorReading>("sideOutPut") {
        });

        //8.打印
        result.print("result");
        sideOutput.print("sideOutput");

        //9.执行任务
        env.execute();

    }

}
