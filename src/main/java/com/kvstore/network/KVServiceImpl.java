package com.kvstore.network;

import com.kvstore.consensus.LogEntry;
import com.kvstore.consensus.RaftNode;
import com.kvstore.grpc.*;
import com.kvstore.storage.StorageEngine;
import io.grpc.stub.StreamObserver;

import com.kvstore.storage.MemTable;

public class KVServiceImpl extends KVServiceGrpc.KVServiceImplBase{
    private StorageEngine engine;
    private RaftNode raftNode;

    public KVServiceImpl(StorageEngine engine,
                         RaftNode raftNode){
        this.engine = engine;
        this.raftNode = raftNode;
    }

    @Override
    public void put(PutRequest request,
                    StreamObserver<PutResponse> streamObserver){
        PutResponse response;

        if(raftNode.getState() != RaftNode.NodeState.LEADER){
            response = PutResponse.newBuilder()
                    .setSuccessful(false)
                    .build();

            streamObserver.onNext(response);
            streamObserver.onCompleted();
            return;
        }

        String command = request.getKey() + ":" + request.getValue();
        boolean success = raftNode.replicateLog(command);
        response = PutResponse.newBuilder().setSuccessful(success).build();

        streamObserver.onNext(response);
        streamObserver.onCompleted();
    }

    @Override
    public void get(GetRequest request,
                    StreamObserver<GetResponse> streamObserver){
        String key = request.getKey();
        String value = engine.get(key);

        GetResponse response;

        if(value != null){
            response = GetResponse.newBuilder()
                    .setValue(value)
                    .setFound(true)
                    .build();
        }else{
            response = GetResponse.newBuilder()
                    .setValue("")
                    .setFound(false)
                    .build();
        }

        streamObserver.onNext(response);
        streamObserver.onCompleted();
    }

    @Override
    public void appendEntries(AppendEntriesRequest request,
                              StreamObserver<AppendEntriesResponse> streamObserver){
        AppendEntriesResponse response = raftNode.handleAppendEntry(request);

        streamObserver.onNext(response);
        streamObserver.onCompleted();
    }

    @Override
    public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> streamObserver){
        RequestVoteResponse response = raftNode.handleVoteRequest(request);

        streamObserver.onNext(response);
        streamObserver.onCompleted();
    }
}
