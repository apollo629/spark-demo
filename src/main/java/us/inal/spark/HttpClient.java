package com.sekomy.psl;

import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import java.util.ArrayList;
import java.util.List;


public class HttpClient {
    public void post(String url, int status) {
        CloseableHttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(url);
        List<BasicNameValuePair> urlParameters = new ArrayList<>();
        urlParameters.add(new BasicNameValuePair("status", String.valueOf(status)));
        try {
            post.setEntity(new UrlEncodedFormEntity(urlParameters));
            client.execute(post);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public void post(String url, int status, ArrayList<String> file_names) {
        CloseableHttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(url);
        List<BasicNameValuePair> urlParameters = new ArrayList<>();
        urlParameters.add(new BasicNameValuePair("status", String.valueOf(status)));
        urlParameters.add(new BasicNameValuePair("file_names", String.valueOf(file_names)));
        try {
            post.setEntity(new UrlEncodedFormEntity(urlParameters));
            client.execute(post);
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}

