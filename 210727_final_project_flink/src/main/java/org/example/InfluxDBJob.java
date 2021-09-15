package org.example;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBConfig;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBPoint;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBSink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class InfluxDBJob {

    private static final int N = 10000;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<String> dataList = new ArrayList<>();
        for (int i = 0; i < N; ++i) {
            String id = "server" + String.valueOf(i);
            dataList.add("cpu#" + id);
            dataList.add("mem#" + id);
            dataList.add("disk#" + id);
        }
        DataStream<String> source = env.fromElements(dataList.toArray(new String[0]));

        DataStream<InfluxDBPoint> dataStream = source.map(
                new RichMapFunction<String, InfluxDBPoint>() {
                    @Override
                    public InfluxDBPoint map(String s) throws Exception {
                        String[] input = s.split("#");

                        String measurement = input[0];
                        long timestamp = System.currentTimeMillis();

                        HashMap<String, String> tags = new HashMap<>();
                        tags.put("host", input[1]);
                        tags.put("region", "region#" + String.valueOf(input[1].hashCode() % 20));

                        HashMap<String, Object> fields = new HashMap<>();
                        fields.put("value1", input[1].hashCode() % 100);
                        fields.put("value2", input[1].hashCode() % 50);

                        return new InfluxDBPoint(measurement, timestamp, tags, fields);
                    }
                }
        );

        InfluxDBConfig influxDBConfig = InfluxDBConfig.builder("http://52.149.146.199:8086", "adminuser", "adminuser", "test")
                .batchActions(1000)
                .flushDuration(100, TimeUnit.MILLISECONDS)
                .enableGzip(true)
                .build();

        dataStream.addSink(new InfluxDBSink(influxDBConfig));

        env.execute("InfluxDB Sink Example");
    }
}
