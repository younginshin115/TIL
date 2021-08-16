package org.example;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBConfig;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBPoint;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaToInfluxDB {
    public static final String KAFKA_BROKER = "kafka:9092";
    public static final String KAFKA_TOPIC1 = "inchat";
    public static final String KAFKA_TOPIC2 = "outchat";

    public static final String INFLUXDB_URL = "http://influxdb:8086";
    public static final String USERNAME = "adminuser";
    public static final String PASSWORD = "adminuser";
    public static final String DBNAME = "kafkaFlink";

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // kafka 환경설정
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KAFKA_BROKER);
        properties.setProperty("group.id", "javas");

        // influxDB 환경설정
        InfluxDBConfig influxDBConfig = InfluxDBConfig.builder(INFLUXDB_URL, USERNAME, PASSWORD, DBNAME)
                .batchActions(1000)
                .flushDuration(100, TimeUnit.MILLISECONDS)
                .enableGzip(true)
                .build();

        // inchat 토픽 데이터량(byte), 개수
        FlinkKafkaConsumer<String> kafkaSource1 = new FlinkKafkaConsumer<String>(KAFKA_TOPIC1, new SimpleStringSchema(), properties);
        kafkaSource1.setStartFromEarliest();
        DataStream<String> kafkaStream1 = env.addSource(kafkaSource1);

        DataStream<InfluxDBPoint> influxDBkafka1 = kafkaStream1.map(
                new RichMapFunction<String, InfluxDBPoint>() {
                    @Override
                    public InfluxDBPoint map(String s) throws Exception {

                        String measurement = "kafkaFlink";
                        long timestamp = System.currentTimeMillis();

                        HashMap<String, String> tags = new HashMap<>();
                        tags.put("topic", "inchat");

                        HashMap<String, Object> fields = new HashMap<>();
                        fields.put("bytes", s.getBytes(StandardCharsets.UTF_8).length);
                        fields.put("count", 1L);

                        return new InfluxDBPoint(measurement, timestamp, tags, fields);
                    }
                }
        );

        // outchat 토픽 데이터량(byte), 개수
        FlinkKafkaConsumer<String> kafkaSource2 = new FlinkKafkaConsumer<String>(KAFKA_TOPIC2, new SimpleStringSchema(), properties);
        kafkaSource2.setStartFromEarliest();
        DataStream<String> kafkaStream2 = env.addSource(kafkaSource2);

        DataStream<InfluxDBPoint> influxDBkafka2 = kafkaStream2.map(
                new RichMapFunction<String, InfluxDBPoint>() {
                    @Override
                    public InfluxDBPoint map(String s) throws Exception {

                        String measurement = "kafkaFlink";
                        long timestamp = System.currentTimeMillis();

                        HashMap<String, String> tags = new HashMap<>();
                        tags.put("topic", "outchat");

                        HashMap<String, Object> fields = new HashMap<>();
                        fields.put("bytes", s.getBytes(StandardCharsets.UTF_8).length);
                        fields.put("count", 1L);

                        return new InfluxDBPoint(measurement, timestamp, tags, fields);
                    }
                }
        );

        // influxDB 환경설정을 적용하여 sink add
        influxDBkafka1.addSink(new InfluxDBSink(influxDBConfig));
        influxDBkafka2.addSink(new InfluxDBSink(influxDBConfig));

        // 실행
        env.execute("Kafka InfluxDB Sink");

    }

}
