package com.redhat.summit2019;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import okhttp3.Credentials;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class Main {

    private static final OkHttpClient http = new OkHttpClient();

    public static void main(String[] args) throws IOException, InterruptedException {
        String baseURL = "http://192.168.0.23:8080/kie-server/services/rest/server";
        String containerId = "kafka-jbpm-process_1.0.23-SNAPSHOT";
        Headers authHeader = new Headers.Builder()
                .add("Authorization", Credentials.basic("wbadmin", "wbadmin"))
                .build();

        URL url = new URL(baseURL + "/queries/tasks/instances/pot-owners?status=Ready&page=0&pageSize=10&sortOrder=true");

        Request request = new Request.Builder()
                .url(url)
                .headers(authHeader)
                .addHeader("Accept", "application/json")
                .get()
                .build();
        Response response = http.newCall(request).execute();
        String json = response.body() != null ? response.body().string() : "";

        JsonFactory jsonFactory = new JsonFactory();
        JsonParser parser = jsonFactory.createParser(json);

        List<Integer> taskIdList = new ArrayList<>();

        while (parser.nextToken() != null) {
            String field = parser.getCurrentName();

            if ("task-id".equalsIgnoreCase(field)) {
                parser.nextToken();
                taskIdList.add(parser.getIntValue());
            }
        }

        parser.close();

        System.out.println("Collected task IDs: " + taskIdList);

        if (taskIdList.isEmpty()) {
            System.out.println("No task IDs at ready state. Aborting...");
            System.exit(-1);
        }

        Thread.sleep(5000);

        System.out.println("Claiming tasks...");
        for (int taskId : taskIdList) {
            url = new URL(baseURL + "/containers/" + containerId + "/tasks/" + taskId + "/states/claimed");
            request = new Request.Builder()
                    .url(url)
                    .headers(authHeader)
                    .put(RequestBody.create(MediaType.get("application/json"), ""))
                    .build();
            response = http.newCall(request).execute();
            if (response.code() != 201) {
                System.out.println(response.body().string());
            }
        }

        Thread.sleep(5000);

        System.out.println("Starting tasks...");
        for (int taskId : taskIdList) {
            url = new URL(baseURL + "/containers/" + containerId + "/tasks/" + taskId + "/states/started");
            request = new Request.Builder()
                    .url(url)
                    .headers(authHeader)
                    .put(RequestBody.create(MediaType.get("application/json"), ""))
                    .build();
            response = http.newCall(request).execute();
            if (response.code() != 201) {
                System.out.println(response.body().string());
            }
        }

        Thread.sleep(5000);

        System.out.println("Completing tasks...");
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int taskId : taskIdList) {
            boolean approved = random.nextBoolean();
            String content = "{\"isApproved\": " + approved + "}";
            url = new URL(baseURL + "/containers/" + containerId + "/tasks/" + taskId + "/states/completed");
            request = new Request.Builder()
                    .url(url)
                    .headers(authHeader)
                    .put(RequestBody.create(MediaType.get("application/json"), content))
                    .build();
            response = http.newCall(request).execute();
            if (response.code() != 201) {
                System.out.println(response.body().string());
            }
        }
    }
}
