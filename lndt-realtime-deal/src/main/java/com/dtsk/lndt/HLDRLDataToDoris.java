package com.dtsk.lndt;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;


/**
 * 葫芦岛热力点表数据处理，初版程序，执行缓慢，没有做任何的优化
 * 生产环境建议使用HLDRLDataToDorisV1
 */
public class HLDRLDataToDoris {

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

        String createTableSQL = "CREATE TABLE kafka_source (\n" +
                "    collect_time TIMESTAMP(3),\n" +
                "    id INT,\n" +
                "    point_value DOUBLE,\n" +
                "    real_time TIMESTAMP(3),\n" +
                "    status INT,\n" +
                "    proctime AS PROCTIME()\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'hldrl_udp_point_table',\n" +
                "    'properties.bootstrap.servers' = '10.217.6.104:9092',\n" +
                "    'properties.group.id' = 'hldrl_udp_point_table_group01',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'format' = 'json'\n" +
                ");\n" +
                "CREATE TABLE hldrl_point_table (\n" +
                "    id INT,\n" +
                "    point_id INT,\n" +
                "    point_name STRING,\n" +
                "    point_index_name STRING,\n" +
                "    heat_station_code INT,\n" +
                "    heat_station_name STRING,\n" +
                "    area_code INT\n" +
                ") \n" +
                "WITH (\n" +
                "    'connector' = 'doris',\n" +
                "    'fenodes' = '10.217.6.103:8030',\n" +
                "    'jdbc-url' = 'jdbc:mysql://10.217.6.103:9030',\n" +
                "    'table.identifier' = 'sfrl.dim_hldrl_point_table',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'DT@lnfgs#2024'\n" +
                ");\n" +
                "CREATE TABLE hldrl_point_table_index (\n" +
                "    point_id INT,\n" +
                "    point_name STRING,\n" +
                "    point_value DOUBLE,\n" +
                "    point_status INT,\n" +
                "    point_index_name STRING,\n" +
                "    heat_station_code INT,\n" +
                "    heat_station_name STRING,\n" +
                "    area_code INT,\n" +
                "    collect_time STRING,\n" +
                "    real_time STRING\n" +
                ") \n" +
                "WITH (\n" +
                "    'connector' = 'doris',\n" +
                "    'fenodes' = '10.217.6.103:8030',\n" +
                "    'jdbc-url' = 'jdbc:mysql://10.217.6.103:9030',\n" +
                "    'table.identifier' = 'sfrl.hldrl_point_table_index',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'DT@lnfgs#2024',\n" +
                "    'sink.label-prefix' = 'doris_label_0002',\n" +
                "    'sink.buffer-flush.max-rows' = '10000',\n" +
                "    'sink.buffer-flush.interval' = '2s'\n" +
                ")";
        Arrays.stream(createTableSQL.split(";"))
                .forEach(tableEnv::executeSql);

//        String executeSQL="INSERT INTO hldrl_point_table_index SELECT d.point_id,d.point_name,f.point_value,f.status point_status,d.point_index_name,d.heat_station_code,d.heat_station_name,d.area_code, CAST(f.collect_time AS STRING) collect_time, CAST(f.real_time AS STRING) real_time FROM kafka_source f JOIN hldrl_point_table FOR SYSTEM_TIME AS OF f.proctime AS d ON f.id = d.point_id";
//        tableEnv.executeSql(executeSQL);

        String querySQL="SELECT d.point_id,d.point_name,f.point_value,f.status point_status,d.point_index_name,d.heat_station_code,d.heat_station_name,d.area_code, CAST(f.collect_time AS STRING) collect_time, CAST(f.real_time AS STRING) real_time FROM kafka_source f JOIN hldrl_point_table FOR SYSTEM_TIME AS OF f.proctime AS d ON f.id = d.point_id";
        TableResult execute = tableEnv.sqlQuery(querySQL).execute();
        execute.collect().forEachRemaining(System.out::println);
    }

    public static void main(String[] args) throws IOException {
        HLDRLDataToDoris hldrlDataToDoris = new HLDRLDataToDoris();
        hldrlDataToDoris.init(args[0]);
        hldrlDataToDoris.run();
    }
}
