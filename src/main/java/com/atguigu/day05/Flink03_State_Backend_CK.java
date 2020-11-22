package com.atguigu.day05;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class Flink03_State_Backend_CK {

    public static void main(String[] args) throws IOException {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //配置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://"));
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new RocksDBStateBackend(""));

        //CK相关设置
        env.enableCheckpointing(10000L);
        env.getCheckpointConfig().setCheckpointInterval(10000L);
        env.getCheckpointConfig().setCheckpointTimeout(1000000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);


    }

}
