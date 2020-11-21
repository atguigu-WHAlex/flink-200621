package com.atguigu.day05;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink01_State_Temp {

    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据创建流
        SingleOutputStreamOperator<SensorReading> sensorDataStream = env.socketTextStream("hadoop102", 7777).map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        //3.分组
        KeyedStream<SensorReading, Tuple> keyedStream = sensorDataStream.keyBy("id");

        //4.判断温度是否跳变,如果跳变超过10度,则报警
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> result = keyedStream.flatMap(new MyTempIncFunc());

        //5.打印输出
        result.print();

        //6.执行任务
        env.execute();

    }

    public static class MyTempIncFunc extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

        //声明上一次温度值的状态
        private ValueState<Double> lastTempState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            //给状态做初始化
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class));
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {

            //a.获取上一次温度值以及当前的温度值
            Double lastTemp = lastTempState.value();
            Double curTemp = value.getTemp();

            //b.判断跳变是否超过10度
            if (lastTemp != null && Math.abs(lastTemp - curTemp) > 10.0) {
                out.collect(new Tuple3<>(value.getId(), lastTemp, curTemp));
            }

            //c.更新状态
            lastTempState.update(curTemp);

        }
    }


}
