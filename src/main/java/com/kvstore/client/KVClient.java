package com.kvstore.client;

import com.kvstore.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KVClient implements AutoCloseable{
    private final List<Integer> ports;
    private final boolean verbose;

    private final Map<Integer, ManagedChannel> channels;
    private final Map<Integer, KVServiceGrpc.KVServiceBlockingStub> stubs;
    private volatile Integer cachedLeaderPort = null;

    public KVClient(List<Integer> ports) {
        this(ports, false);
    }

    public KVClient(List<Integer> ports, boolean verbose) {
        this.ports = ports;
        this.verbose = verbose;

        this.channels = new ConcurrentHashMap<>();
        this.stubs = new ConcurrentHashMap<>();
        for (Integer port : ports) {
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress("localhost", port)
                    .usePlaintext()
                    .build();
            channels.put(port, channel);
            stubs.put(port, KVServiceGrpc.newBlockingStub(channel));
        }
    }

    public boolean put(String key, String value) {
        PutRequest request = PutRequest.newBuilder()
                .setKey(key)
                .setValue(value)
                .build();

        if(cachedLeaderPort != null){
            try {
                KVServiceGrpc.KVServiceBlockingStub leaderStub = stubs.get(cachedLeaderPort);

                PutResponse response = leaderStub.put(request);
                if (response.getSuccessful()) {
                    return true;
                }
            } catch (Exception e) {
                if(verbose){
                    System.out.println("Leader is unreachable");
                }
            }

            cachedLeaderPort = null;
        }

        for (Integer port : ports) {
            KVServiceGrpc.KVServiceBlockingStub stub = stubs.get(port);

            if(stub == null){
                continue;
            }

            try {
                PutResponse response = stub.put(request);
                if (response.getSuccessful()) {
                    if (verbose) {
                        System.out.println("Success on port: " + port);
                    }
                    cachedLeaderPort = port;
                    return true;
                }
            } catch (Exception e) {
                if (verbose) {
                    System.out.println("Node " + port + " is down. Retrying...");
                }
            }
        }

        if (verbose) {
            System.err.println("Error: could not find leader");
        }
        return false;
    }

    public String get(String key) {
        GetRequest request = GetRequest.newBuilder()
                .setKey(key)
                .build();

        if(cachedLeaderPort != null){
            try {
                KVServiceGrpc.KVServiceBlockingStub leaderStub = stubs.get(cachedLeaderPort);
                GetResponse response = leaderStub.get(request);

                if (response.getSuccessful()) {
                    if (response.getFound()) {
                        return response.getValue();
                    } else{
                        if(verbose){
                            System.out.println("Key could not be found.");
                        }
                        return null;
                    }
                }
            } catch (Exception e) {
                if(verbose){
                    System.out.println("Leader is unreachable");
                }
            }

            cachedLeaderPort = null;
        }

        for (Integer port : ports) {
            KVServiceGrpc.KVServiceBlockingStub stub = stubs.get(port);

            if(stub == null){
                continue;
            }

            try {
                GetResponse response = stub.get(request);
                if (response.getSuccessful()) {
                    cachedLeaderPort = port;

                    if (response.getFound()) {
                        if (verbose) {
                            System.out.println("Success on port: " + port);
                        }
                        return response.getValue();
                    } else {
                        if (verbose) {
                            System.out.println("Record does not exist.");
                        }
                        return null;
                    }
                }
            } catch (Exception e) {
                if (verbose) {
                    System.out.println("Node " + port + " is down. Retrying...");
                }
            }
        }
        if (verbose) {
            System.err.println("Error: No record exists or leader could not be found");
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        for (ManagedChannel channel : channels.values()) {
            channel.shutdown();
        }
    }
}
