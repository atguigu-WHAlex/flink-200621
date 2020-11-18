package com.atguigu;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Collections;
import java.util.Properties;

public class Test02_KafkaSource_Split {

    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.读取Kafka数据创建流
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer011<String>("test", new SimpleStringSchema(), properties));

        //3.分流
        SplitStream<String> split = kafkaDS.split(new OutputSelector<String>() {
            @Override
            public Iterable<String> select(String value) {
                //获取温度
                double temp = Double.parseDouble(value.split(",")[2]);
                return temp > 30.0 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        //4.选择流
        split.select("high").print("high");
        split.select("low").print("low");

        //5.执行任务
        env.execute();

    }

}
