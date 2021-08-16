package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBConfig;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class SparkMetricsToInfluxDB {

    private static final String SPARKURL = "http://spark-master:4040/api/v1/applications";

    public static final String INFLUXDB_URL = "http://influxdb:8086";
    public static final String USERNAME = "adminuser";
    public static final String PASSWORD = "adminuser";
    public static final String DBNAME = "test";

    private static HttpURLConnection httpURLConnection(String str) throws MalformedURLException, IOException {

        /* http 연결 코드 반환 함수 */

        URL url = new URL(str);
        return (HttpURLConnection) url.openConnection();
    }

    private static int getResponseStatus(String str) throws MalformedURLException, IOException {

        /* http 상태 코드(연결 상태) 반환 함수 */

        URL url = new URL(str);
        HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
        return httpConn.getResponseCode();
    }

    private static String getUrlWithId(BufferedReader br) throws MalformedURLException, IOException, ParseException {

        /* 현재 Job의 ID를 더해 새로운 URL을 반환하는 함수 : Job이 변경되더라도 대응할 수 있도록 개발 */
        
        JSONParser jsonParser = new JSONParser();
        JSONArray jsonArray = (JSONArray) jsonParser.parse(br);
        
        JSONObject jsonObject = (JSONObject) jsonArray.get(0);

        return SPARKURL + "/" + jsonObject.get("id").toString();
    }

    private static HashMap<String, Long> getExecutorsData(BufferedReader br) throws MalformedURLException, IOException, ParseException {
        
        /* executors endpoints를 통해 failed tasks, completed tasks, total tasks 반환하는 함수 */
        
        JSONParser jsonParser = new JSONParser();
        JSONArray jsonArray = (JSONArray) jsonParser.parse(br);
        
        JSONObject jsonObject = (JSONObject) jsonArray.get(0);

        HashMap<String, Long> outputObj = new HashMap<>();
        outputObj.put("failedTasks", (Long) jsonObject.get("failedTasks"));
        outputObj.put("completedTasks", (Long) jsonObject.get("completedTasks"));
        outputObj.put("totalTasks", (Long) jsonObject.get("totalTasks"));

        return outputObj;
    }

    private static ArrayList<HashMap<String, Object>> getStagesData(BufferedReader br) throws MalformedURLException, IOException, ParseException {

        JSONParser jsonParser = new JSONParser();
        JSONArray jsonArray = (JSONArray) jsonParser.parse(br);

        ArrayList<HashMap<String, Object>> stageList = new ArrayList<>();

        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject jsonObject = (JSONObject) jsonArray.get(i);

            HashMap<String, Object> jsonHashMap = new HashMap<>();
            jsonHashMap.put("stageId", jsonObject.get("stageId"));
            jsonHashMap.put("submissionTime", jsonObject.get("submissionTime"));
            jsonHashMap.put("completionTime", jsonObject.get("completionTime"));
            jsonHashMap.put("executorRunTime", (long) jsonObject.get("executorRunTime") / 1000);
            jsonHashMap.put("resultSize", jsonObject.get("resultSize"));
            jsonHashMap.put("inputBytes", jsonObject.get("inputBytes"));
            jsonHashMap.put("outputBytes", jsonObject.get("outputBytes"));
            jsonHashMap.put("outputRecords", jsonObject.get("outputRecords"));

            stageList.add(jsonHashMap);
        }

        return stageList;
    }

    public static void main(String[] args) throws MalformedURLException, IOException, ParseException {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // influxDB 환경설정
        InfluxDBConfig influxDBConfig = InfluxDBConfig.builder(INFLUXDB_URL, USERNAME, PASSWORD, DBNAME)
                .batchActions(1000)
                .flushDuration(100, TimeUnit.MILLISECONDS)
                .enableGzip(true)
                .build();

        // 연결 객체 생성

        HttpURLConnection httpConn = httpURLConnection(SPARKURL);
        int statusCode = getResponseStatus(SPARKURL);

        // if문 안에서 사용할 변수 선언
        
        String urlWithId = null;
        HashMap<String, Long> executorData = new JSONObject();
        ArrayList<HashMap<String, Object>> stageData = new ArrayList<>();

        // 연결이 되었을 경우 함수 진행
        

        BufferedReader br = new BufferedReader(new InputStreamReader(httpConn.getInputStream()));
        urlWithId = getUrlWithId(br);

        HttpURLConnection httpConnExecutor = httpURLConnection(urlWithId + "/allexecutors");
        BufferedReader brEx = new BufferedReader(new InputStreamReader(httpConnExecutor.getInputStream()));
        executorData = getExecutorsData(brEx);

        // batch 데이터 당 정보

        HttpURLConnection httpConnStage = httpURLConnection(urlWithId + "/stages");
        BufferedReader brSt = new BufferedReader(new InputStreamReader(httpConnStage.getInputStream()));
        JSONParser jsonParser = new JSONParser();
        JSONArray jsonArray = (JSONArray) jsonParser.parse(brSt);


        ArrayList<HashMap<String, Object>> stageList = new ArrayList<>();
        for (Object o : jsonArray) {
            JSONObject jsonObject = (JSONObject) o;
            HashMap<String, Object> stageHashMap = new HashMap<>();
            stageHashMap.put("stageId", jsonObject.get("stageId"));
            stageHashMap.put("submissionTime", jsonObject.get("submissionTime"));
            stageHashMap.put("completionTime", jsonObject.get("completionTime"));
            stageHashMap.put("executorRunTime", (long) jsonObject.get("executorRunTime") / 1000);
            stageHashMap.put("resultSize", jsonObject.get("resultSize"));
            stageHashMap.put("inputBytes", jsonObject.get("inputBytes"));
            stageHashMap.put("outputBytes", jsonObject.get("outputBytes"));
            stageHashMap.put("outputRecords", jsonObject.get("outputRecords"));
            stageList.add(stageHashMap);
        }


        /*
        DataStream<String> source = env.fromElements(stageList);
        DataStream<InfluxDBPoint> dataStream = source.map(
                (RichMapFunction) (s) -> {
                    String measurement = "SparkTest";
                    Long timestamp =


                }
        )
        
         */


    }

}
