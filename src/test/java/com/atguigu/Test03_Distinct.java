package com.atguigu;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

public class Test03_Distinct {

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

        //4.过滤
        SingleOutputStreamOperator<String> filter = wordDS.filter(new RichFilterFunction<String>() {
            //声明Redis连接
            Jedis jedis = null;
            //定义Set  RedisKey
            String redisKey = "distinct";

            @Override
            public void open(Configuration parameters) throws Exception {
                jedis = new Jedis("hadoop102", 6379);
            }

            @Override
            public boolean filter(String value) throws Exception {
                //查询Redis中是否存在该单词
                Boolean exist = jedis.sismember(redisKey, value);
                //将该单词写入Redis
                if (!exist) {
                    jedis.sadd(redisKey, value);
                }
                //返回
                return !exist;
            }

            @Override
            public void close() throws Exception {
                jedis.close();
            }
        });

        //5.打印数据
        filter.print();

        //6.执行任务
        env.execute();

    }

}
