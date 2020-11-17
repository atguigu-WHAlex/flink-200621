package com.atguigu.day02;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

public class Flink04_Source_Customer {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从自定义的Source中读取数据
        DataStreamSource<SensorReading> mySourceDS = env.addSource(new CustomerSource());

        //3.打印
        mySourceDS.print();

        //4.启动任务
        env.execute("Flink04_Source_Customer");
    }

    public static class CustomerSource implements SourceFunction<SensorReading> {

        //定义标志位控制数据接收
        private boolean running = true;

        Random random = new Random();

        @Override
        public void run(SourceContext ctx) throws Exception {

            //定义Map
            HashMap<String, Double> tempMap = new HashMap<>();

            //向map中添加基准值
            for (int i = 0; i < 10; i++) {
                tempMap.put("Sensor_" + i, 50 + random.nextGaussian() * 20);
            }

            while (running) {

                //遍历Map
                for (String id : tempMap.keySet()) {

                    //提取上一次当前传感器温度
                    Double temp = tempMap.get(id);

                    double newTemp = temp + random.nextGaussian();
                    ctx.collect(new SensorReading(id, System.currentTimeMillis(), newTemp));

                    //将当前温度设置进Map,给下一次作为基准
                    tempMap.put(id, newTemp);
                }

                Thread.sleep(2000);
            }

        }

        @Override
        public void cancel() {
            running = false;
        }
    }


}
