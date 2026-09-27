package com.kvstore.client;

import com.kvstore.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.List;

public class KVClient {
    private final List<Integer> ports;
    private final boolean verbose;

    public KVClient(List<Integer> ports) {
        this(ports, false);
    }

    public KVClient(List<Integer> ports, boolean verbose) {
        this.ports = ports;
        this.verbose = verbose;
    }

    public boolean put(String key, String value) {
        for (Integer port : ports) {
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress("localhost", port)
                    .usePlaintext()
                    .build();

            KVServiceGrpc.KVServiceBlockingStub stub = KVServiceGrpc.newBlockingStub(channel);
            PutRequest request = PutRequest.newBuilder()
                    .setKey(key)
                    .setValue(value)
                    .build();
            try {
                PutResponse response = stub.put(request);
                if (response.getSuccessful()) {
                    if (verbose) {
                        System.out.println("Success on port: " + port);
                    }
                    return true;
                }
            } catch (Exception e) {
                if (verbose) {
                    System.out.println("Node " + port + " is down. Retrying...");
                }
            } finally {
                channel.shutdown();
            }
        }

        if (verbose) {
            System.err.println("Error: could not find leader");
        }
        return false;
    }

    public String get(String key) {
        for (Integer port : ports) {
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress("localhost", port)
                    .usePlaintext()
                    .build();
            KVServiceGrpc.KVServiceBlockingStub stub = KVServiceGrpc.newBlockingStub(channel);

            GetRequest request = GetRequest.newBuilder()
                    .setKey(key)
                    .build();

            try {
                GetResponse response = stub.get(request);
                if (response.getSuccessful()) {
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
            } finally {
                channel.shutdown();
            }
        }
        if (verbose) {
            System.err.println("Error: No record exists or leader could not be found");
        }
        return null;
    }
}
