package com.example.demo;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@RestController
public class Service {

    /**
     * There typically needs to be only one Cluster instance in an application.
     */
    static Cluster cluster;

    /**
     * Use the Cluster instance to construct different Client instances (e.g. one for sessionless communication
     * and one or more sessions). A sessionless Client should be thread-safe and typically no more than one is
     * needed unless there is some need to divide connection pools across multiple Client instances. In this case
     * there is just a single sessionless Client instance used for the entire App.
     */
    static Client client;


    @GetMapping("/test")
    Result all() {


        try {
            // Attempt to create the connection objects
            cluster = Cluster.build(new File("src/main/resources/remote.yaml")).create();
            client = cluster.connect();
        } catch (FileNotFoundException e) {
            // Handle file errors.
            System.out.println("Couldn't find the configuration file.");
            e.printStackTrace();
            return null;
        }

        List<Result> result =  execute("g.V()");

        for (Result r :  result) {
            System.out.println("\nQuery result:");
            System.out.println(r.toString());
        }

        // Status code for successful query. Usually HTTP 200.
        //   System.out.println("Status: " + statusAttributes.get("x-ms-status-code").toString());

        // Total Request Units (RUs) charged for the operation, after a successful run.
        // System.out.println("Total charge: " + statusAttributes.get("x-ms-total-request-charge").toString());


        // Properly close all opened clients and the cluster
        cluster.close();

        return result.get(0);
    }



    private List<Result> execute(String query) {

        System.out.println("\nSubmitting this Gremlin query: " + query);

        // Submitting remote query to the server.
        ResultSet results = client.submit(query);

        CompletableFuture<List<Result>> completableFutureResults;
        CompletableFuture<Map<String, Object>> completableFutureStatusAttributes;
        List<Result> resultList;
        Map<String, Object> statusAttributes;

        try{
            completableFutureResults = results.all();
            completableFutureStatusAttributes = results.statusAttributes();
            resultList = completableFutureResults.get();
            statusAttributes = completableFutureStatusAttributes.get();
            return resultList;
        }
        catch(ExecutionException | InterruptedException e){
            e.printStackTrace();
            return new ArrayList<>() ;
        }
        catch(Exception e){
            ResponseException re = (ResponseException) e.getCause();

            // Response status codes. You can catch the 429 status code response and work on retry logic.
            System.out.println("Status code: " + re.getStatusAttributes().get().get("x-ms-status-code"));
            System.out.println("Substatus code: " + re.getStatusAttributes().get().get("x-ms-substatus-code"));

            // If error code is 429, this value will inform how many milliseconds you need to wait before retrying.
            System.out.println("Retry after (ms): " + re.getStatusAttributes().get().get("x-ms-retry-after"));

            // Total Request Units (RUs) charged for the operation, upon failure.
            System.out.println("Request charge: " + re.getStatusAttributes().get().get("x-ms-total-request-charge"));

            // ActivityId for server-side debugging
            System.out.println("ActivityId: " + re.getStatusAttributes().get().get("x-ms-activity-id"));
            throw(e);
        }

    }
}
