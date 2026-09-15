package com.kvstore.client;

import java.util.ArrayList;
import java.util.Arrays;

public class ChaosClient {
    public static void main(String[] args){
        ArrayList<Integer> ports = new ArrayList<>(Arrays.asList(8081, 8082, 8083, 8084, 8085));
        KVClient kvClient = new KVClient(ports);

        long start = System.currentTimeMillis();
        for(int i=0; i < 2000; i++){
            kvClient.put("key" + i, "data" + i);
        }
        long end = System.currentTimeMillis();

        double seconds = (end - start)/1000.0;
        double throughput = 2000/seconds;
        System.out.println("Throughput: " + throughput + "operations per seconds");
    }
}
