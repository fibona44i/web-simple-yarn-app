package com.hortonworks.simpleyarnapp;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.json.JSONObject;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Andrii_Krasnolob on 2/11/2016.
 */
public class HWebCatMyClient {
    private static final String httpHostPort = new String ("http://sandbox.hortonworks.com:50111/templeton/v1");
    private static final String jobStatusPath = new String("/jobs/");
    private static final String hivePath = new String("/hive/");
    private static final String userName = new String("?user.name=hive");
    private static final String hiveURI = httpHostPort+hivePath+userName;

    public Boolean getJobStatus(String jobID) throws IOException {
        String jobStatusURI =  httpHostPort + jobStatusPath + jobID + userName;
        HttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(jobStatusURI);
        HttpResponse response = client.execute(request);


        BufferedReader rd = new BufferedReader (new InputStreamReader(response.getEntity().getContent()));
        String line = "";
        String jsonResponce = "";
        while ((line = rd.readLine()) != null) {
            jsonResponce += line;
        }
        JSONObject obj = new JSONObject(jsonResponce);
        return obj.getJSONObject("status").getBoolean("jobComplete");

    }
    public String submitQuery(String query, String outputDirectory) throws IOException {

        HttpClient client = HttpClientBuilder.create().build();
        HttpPost request = new HttpPost(hiveURI);
        List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
        urlParameters.add(new BasicNameValuePair("execute", query));
        urlParameters.add(new BasicNameValuePair("statusdir", outputDirectory));

        request.setEntity(new UrlEncodedFormEntity(urlParameters));
        HttpResponse response = client.execute(request);

        BufferedReader rd = new BufferedReader (new InputStreamReader(response.getEntity().getContent()));
        String line = "";
        String jsonResponce = "";
        while ((line = rd.readLine()) != null) {
            jsonResponce += line;
        }
        JSONObject obj = new JSONObject(jsonResponce);
        return obj.getString("id");

    }
}
