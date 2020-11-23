package com.atguigu.day06;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class FlinkSQL02_Env {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.基于老版本的流式处理环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useOldPlanner()      // 使用老版本planner
                .inStreamingMode()    // 流处理模式
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //2.基于老版本的批处理环境
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment batchTableEnv = BatchTableEnvironment.create(batchEnv);

        //3.基于新版本的流式处理环境
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(env, bsSettings);

        //4.基于新版本的批处理环境
        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment bbTableEnv = TableEnvironment.create(bbSettings);


    }

}
