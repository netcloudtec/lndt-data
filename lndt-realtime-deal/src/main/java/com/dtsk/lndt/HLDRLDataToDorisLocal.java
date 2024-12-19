package com.dtsk.lndt;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author netcloud
 * @date 2024-12-18 21:41:28
 * @email netcloudtec@163.com
 * @description 本地测试FlinkSQL的LookUPJoin和Sink到Doris
 */
public class HLDRLDataToDorisLocal {
    public final Logger logger = LoggerFactory.getLogger(getClass().getName());
    private static String flinkCheckpointDir;
    private static long checkpointIntervalMS;
    private static int checkPointTimeout;
    private static int restartBetweenTime;
    private static int checkpointsBetweenTime;
    private static int maxConcurrentCheckpoints;
    private static int restartNum;
    private static boolean checkpoint;

    //初始化参数
    public void init(String fileName) throws IOException {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(fileName);
        ParameterTool parameters = ParameterTool.fromPropertiesFile(inputStream);
        flinkCheckpointDir = parameters.get("flink.checkpoint.dir");
        checkpointIntervalMS = parameters.getLong("flink.checkpoint.interval.time");
        checkPointTimeout = parameters.getInt("flink.checkpoint.timeout");
        restartBetweenTime = parameters.getInt("flink.restart.between.time");
        checkpointsBetweenTime = parameters.getInt("flink.checkpoints.between.time");
        maxConcurrentCheckpoints = parameters.getInt("flink.max.concurrent.checkpoints");
        restartNum = parameters.getInt("flink.restart.num");
        checkpoint = parameters.getBoolean("flink.checkpoint");
    }

    public void run() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(10);
        //运行环境并设置checkpoint
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(checkpointIntervalMS);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(checkPointTimeout);
        //设置Flink的重启策略,重试4次,每次间隔1秒
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(restartNum, restartBetweenTime));
        //确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(checkpointsBetweenTime);
        //同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(maxConcurrentCheckpoints);
        if (checkpoint) {
            env.setStateBackend(new HashMapStateBackend());
            env.getCheckpointConfig().setCheckpointStorage(flinkCheckpointDir);
            env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        }
        String datagenSQL = "CREATE TABLE dataGenSourceTable (\n" +
                "id INT,\n" +
                "proctime AS PROCTIME()\n" +
                ") WITH ( \n" +
                "'connector'='datagen', \n" +
                "'fields.id.kind'='random',\n" +
                "'fields.id.min'='1',\n" +
                "'fields.id.max'='10'\n" +
                ")";
        tableEnv.executeSql(datagenSQL);

        String dimSQL = "CREATE TABLE dim_hldrl_point_table (\n" +
                "id INT,\n" +
                "desc STRING\n" +
                ")\n" +
                "WITH (\n" +
                "    'connector' = 'doris',\n" +
                "    'fenodes' = '172.16.240.22:8030',\n" +
                "    'jdbc-url' = 'jdbc:mysql://172.16.240.22:9030',\n" +
                "    'table.identifier' = 'demo.dim_hldrl_point_table',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'Sunmnet@123'\n" +
                "    'lookup.cache.max-rows' = '5000',\n" +
                "    'lookup.cache.ttl' = '24h',\n" +
                "    'lookup.max-retries' = '3'\n" +
                ")";
        tableEnv.executeSql(dimSQL);

        String sinkTable = "CREATE TABLE hldrl_point_table_index (\n" +
                "id INT,\n" +
                "desc STRING\n" +
                ")\n" +
                "WITH (\n" +
                "    'connector' = 'doris',\n" +
                "    'fenodes' = '172.16.240.22:8030',\n" +
                "    'jdbc-url' = 'jdbc:mysql://172.16.240.22:9030',\n" +
                "    'table.identifier' = 'demo.hldrl_point_table_index',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'Sunmnet@123',\n" +
                "    'sink.label-prefix' = 'doris_label_0004',\n" +
                "    'sink.enable.batch-mode' = 'true',\n" +
                "    'sink.buffer-flush.interval' = '5s'\n" +
                ");";
        tableEnv.executeSql(sinkTable);

        String executeSQL = "INSERT INTO hldrl_point_table_index SELECT f.id,d.desc FROM dataGenSourceTable f JOIN dim_hldrl_point_table FOR SYSTEM_TIME AS OF f.proctime AS d ON f.id = d.id";
        String querySQL = "SELECT COUNT(1) FROM (SELECT f.id,d.desc FROM dataGenSourceTable f JOIN dim_hldrl_point_table FOR SYSTEM_TIME AS OF f.proctime AS d ON f.id = d.id) t1";
        tableEnv.executeSql(executeSQL);
        tableEnv.sqlQuery(querySQL).execute().print();
    }

    public static void main(String[] args) throws IOException {
        HLDRLDataToDorisLocal hldrlDataToDoris = new HLDRLDataToDorisLocal();
        hldrlDataToDoris.init(args[0]);
        hldrlDataToDoris.run();
    }
}

