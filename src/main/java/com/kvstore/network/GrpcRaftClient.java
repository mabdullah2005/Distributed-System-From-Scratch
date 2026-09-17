package com.kvstore.network;

import com.kvstore.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class GrpcRaftClient implements RaftRpcClient{
    private ManagedChannel channel;
    private KVServiceGrpc.KVServiceBlockingStub stub;
    private Integer targetPort;

    public GrpcRaftClient(Integer targetPort){
        this.targetPort = targetPort;

        this.channel = ManagedChannelBuilder
                .forAddress("localhost", targetPort)
                .usePlaintext()
                .build();

        this.stub = KVServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public AppendEntriesResponse sendAppendEntries(AppendEntriesRequest request) {
        AppendEntriesResponse response;

        try{
            response = stub.appendEntries(request);

            if(response.getSuccess()){
                System.out.println("Port " + targetPort + " accepted the logs!");
            }
            else{
                System.out.println("Port " + targetPort + " did not accept the logs");
            }
            return response;
        } catch (Exception e) {
            System.out.println("Error: " + targetPort + " did not accept the logs");
        }
        return null;
    }

    @Override
    public RequestVoteResponse sendRequestVote(RequestVoteRequest request) {
        RequestVoteResponse response;

        try{
            response = stub.requestVote(request);

            if(response.getVoteGranted()){
                System.out.println("Port " + targetPort + " granted its vote!");
            } else{
                System.out.println("Port " + targetPort + " did not grant its vote.");
            }

            return response;
        } catch (Exception e) {
            System.out.println("Port " + targetPort + " is not alive");
        }
        return null;
    }
}
