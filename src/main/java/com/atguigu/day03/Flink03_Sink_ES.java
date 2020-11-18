package com.atguigu.day03;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

public class Flink03_Sink_ES {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件中读取数据创建流
        DataStreamSource<String> inputDS = env.readTextFile("sensor");

        //3.将数据写入ES

        //3.1 创建集合用于存放连接条件
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102", 9200));

        //3.2 构建ElasticsearchSink
        ElasticsearchSink<String> elasticsearchSink =
                new ElasticsearchSink.Builder<>(httpHosts, new MyEsSinkFunc())
                        .build();

        //3.3 写入数据操作
        inputDS.addSink(elasticsearchSink);

        //4.执行任务
        env.execute();

    }

    public static class MyEsSinkFunc implements ElasticsearchSinkFunction<String> {

        @Override
        public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {

            //对元素分割处理
            String[] fields = element.split(",");

            //创建Map用于存放待存储到ES的数据
            HashMap<String, String> source = new HashMap<>();
            source.put("id", fields[0]);
            source.put("ts", fields[1]);
            source.put("temp", fields[2]);

            //创建IndexRequest
            IndexRequest indexRequest = Requests.indexRequest()
                    .index("sensor")
                    .type("_doc")
//                    .id(fields[0])
                    .source(source);

            //将数据写入
            indexer.add(indexRequest);

        }
    }
}
