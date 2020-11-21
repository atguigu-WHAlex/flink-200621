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
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink02_State_OnTimer {

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

        //4.判断温度10秒没有下降,则报警
        SingleOutputStreamOperator<String> result = keyedStream.process(new MyKeyedProcessFunc());

        //5.打印输出
        result.print();

        //6.执行任务
        env.execute();

    }

    public static class MyKeyedProcessFunc extends KeyedProcessFunction<Tuple, SensorReading, String> {

        //定义状态
        private ValueState<Double> lastTempState = null;
        private ValueState<Long> tsState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {

            //提取上一次的温度值
            Double lastTemp = lastTempState.value();
            Long lastTs = tsState.value();
            long ts = ctx.timerService().currentProcessingTime() + 5000L;

            //第一条数据,需要注册定时器
            if (lastTs == null) {
                ctx.timerService().registerProcessingTimeTimer(ts);
                tsState.update(ts);
            } else {
                //非第一条数据,则需要判断温度是否下降
                if (lastTemp != null && value.getTemp() < lastTemp) {
                    //删除定时器
                    ctx.timerService().deleteProcessingTimeTimer(tsState.value());
                    //重新注册新的定时器
                    ctx.timerService().registerProcessingTimeTimer(ts);
                    tsState.update(ts);
                }
            }

            //更新状态
            lastTempState.update(value.getTemp());

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey() + "连续10秒温度没有下降");
            tsState.clear();
        }
    }


}
