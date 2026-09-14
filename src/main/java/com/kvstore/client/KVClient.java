package com.kvstore.client;

import com.kvstore.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.List;
import java.util.ArrayList;

public class KVClient {
    private List<Integer> ports;

    public KVClient(ArrayList<Integer> ports){
        this.ports = ports;
    }

    public void put(String key, String value){
        for(Integer port: ports){
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress("localhost", port)
                    .usePlaintext()
                    .build();

            KVServiceGrpc.KVServiceBlockingStub stub = KVServiceGrpc.newBlockingStub(channel);
            PutRequest request = PutRequest.newBuilder()
                    .setKey(key)
                    .setValue(value)
                    .build();
            try{
                PutResponse response = stub.put(request);
                if(response.getSuccessful()){
                    System.out.println("Success on port: " + port);
                    return;
                }
            } catch (Exception e) {
                System.out.println("Node" + port + "is down. Retrying...");
            } finally{
                channel.shutdown();
            }
        }

        System.out.println("Error: could not find leader");
    }

    public String get(String key){
        for(Integer port: ports){
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress("localhost", port)
                    .usePlaintext()
                    .build();
            KVServiceGrpc.KVServiceBlockingStub stub = KVServiceGrpc.newBlockingStub(channel);

            GetRequest request = GetRequest.newBuilder()
                    .setKey(key)
                    .build();

            try{
                GetResponse response = stub.get(request);
                if(response.getFound()){
                    System.out.println("Success on port: " + port);
                    return response.getValue();
                }
            } catch (Exception e) {
                System.out.println("Node" + port + "is down. Retrying...");
            } finally{
                channel.shutdown();
            }
        }
        System.out.println("Error: No record exists or leader could not be found");
        return null;
    }
}
