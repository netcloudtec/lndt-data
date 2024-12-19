FLINK_CMD="/opt/flink-1.16.0/bin/flink run  -c com.dtsk.lndt.HLDRLDataToDorisV1 /data01/lndt/lndt-realtime-deal-jar-with-dependencies.jar"
$FLINK_CMD  config-stage.properties
