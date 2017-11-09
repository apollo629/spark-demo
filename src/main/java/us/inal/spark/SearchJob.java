package com.sekomy.psl;


import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat;
import org.json.JSONArray;
import org.json.JSONObject;
import scala.collection.JavaConversions;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by orionasa on 24/05/17.
 */

public class SearchJob {

    public static void main(String[] args)  {
        JSONObject config = new JSONObject(args[0].toString());
        Map<String, ArrayList<String>> path_list = new HashMap<String, ArrayList<String>>();
        String generatedQuery = "select * from data where ";
        List<String> unique_id_list = new ArrayList<>();
        try {
            for (Object o : config.getJSONArray("path_config")) {
                List<String> path = (new JSONObject(o.toString()).getJSONArray("paths")).toList().stream().map(Object::toString)
                        .collect(Collectors.toList());
                path_list.put(new JSONObject(o.toString()).getString("delimiter"),(ArrayList<String>)path);
            }
            for (int i = 0; i < config.getJSONArray("query").length(); i++) {
                JSONObject field = new JSONObject(config.getJSONArray("query").get(i).toString());
                generatedQuery = generatedQuery + field.get("key") + " like '%" + field.get("value") + "%'";
                if (i != config.getJSONArray("query").length() - 1) {
                    generatedQuery += " and ";
                }
            }
        }catch (Exception e){
            e.printStackTrace();
            new HttpClient().post(config.getString("callback_url"),40);
            throw e;
        }
        try {
//            SparkConf conf = new SparkConf().setAppName("RDD Exmp").setMaster("local[4]").set("spark.executor.memory","514m");
            SparkSession spark = SparkSession
                    .builder()
//                    .config(conf)
                    .getOrCreate();
//            List<Dataset<Row>> ds_list = new ArrayList<Dataset<Row>>();
//            Set<String> column_list = new HashSet<>();
//            Dataset<Row> ds  = spark.read().format(CSVFileFormat.class.getCanonicalName())
//                    .option("header", "true").option("delimiter", config.getString("delimiter"))
//                    .load(JavaConversions.asScalaBuffer(path_list).toSeq());
            for(String key: path_list.keySet()){
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                String unique_id = String.valueOf(timestamp.getTime());
                Dataset<Row> ds  = spark.read().format(CSVFileFormat.class.getCanonicalName())
                        .option("header", "true").option("delimiter", key)
                        .load(JavaConversions.asScalaBuffer(path_list.get(key)).toSeq());
                ds.createOrReplaceTempView("data");

                ds = ds.sqlContext().sql(generatedQuery);
                unique_id_list.add(unique_id);
//                column_list.addAll(Arrays.asList(ds.columns()));
//                ds_list.add(ds);
                ds.repartition(1).write().format(CSVFileFormat.class.getCanonicalName())
                        .save(config.getString("tempdir")+unique_id);
            }
//
//            for (int i = 0; i<ds_list.size();i++){
//                for(String o: column_list) {
//                    if (!Arrays.asList(ds_list.get(i).columns()).contains(o)){
//                        ds_list.set(i, ds_list.get(i).withColumn(o, functions.lit(null)));
//                    }
//                }
//            }
//
//            Dataset<Row> result = spark.emptyDataFrame();
//            for(String o: column_list) {
//                if (!Arrays.asList(result.columns()).contains(o)){
//                    result = result.withColumn(o, functions.lit(null));
//                }
//            }
//            for(Dataset<Row> ds:ds_list){
//                result = result.union(ds);
//            }

        }catch (Exception e){
            e.printStackTrace();
            new HttpClient().post(config.getString("callback_url"),40);
            throw e;
        }

        FTPClient client = new FTPClient();
        FileInputStream fis = null;
        ArrayList<String> file_names = new ArrayList<String>();
        try {
            client.connect(config.getJSONObject("sink").getString("host"),config.getJSONObject("sink")
                    .getInt("port"));
            client.login(config.getJSONObject("sink").getString("username"), config.getJSONObject("sink")
                    .getString("password"));
            FileFilter fileFilter = new WildcardFileFilter("*.csv");
            int part = 0;
            for(String unique_id: unique_id_list) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                File dir = new File(config.getString("tempdir") + unique_id);
                File[] files = dir.listFiles(fileFilter);
                fis = new FileInputStream(files[0].getAbsolutePath());
                String file_name = config.getJSONObject("sink").getString("file_name") + part + "-" +
                        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS").format(timestamp)+".csv";
                String ftp_path = config.getJSONObject("sink").getString("default_path") + file_name;
                file_names.add(file_name);
                client.storeFile(ftp_path, fis);
                fis.close();
                part++;
            }
            new HttpClient().post(config.getString("callback_url"),30,file_names);
            client.logout();
        } catch (IOException e) {
            e.printStackTrace();
            new HttpClient().post(config.getString("callback_url"),40);
        } finally {
            try {
                for (String unique_id: unique_id_list){
                    FileUtils.deleteDirectory(new File(config.getString("tempdir")+unique_id));
                }
                client.disconnect();
            } catch (IOException e) {
                new HttpClient().post(config.getString("callback_url"),40);
                e.printStackTrace();
            }
        }

    }
}
