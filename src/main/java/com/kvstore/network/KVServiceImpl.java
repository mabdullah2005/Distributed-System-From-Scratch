package com.kvstore.network;

import com.kvstore.consensus.LogEntry;
import com.kvstore.consensus.RaftNode;
import com.kvstore.grpc.*;
import io.grpc.stub.StreamObserver;

import com.kvstore.storage.MemTable;

public class KVServiceImpl extends KVServiceGrpc.KVServiceImplBase{
    private MemTable memTable;
    private RaftNode raftNode;

    public KVServiceImpl(MemTable memTable,
                         RaftNode raftNode){
        this.memTable = memTable;
        this.raftNode = raftNode;
    }

    @Override
    public void put(PutRequest request,
                    StreamObserver<PutResponse> streamObserver){
        String key = request.getKey();
        String value = request.getValue();
        memTable.put(key, value);

        PutResponse response = PutResponse.newBuilder()
                .setSuccessful(true)
                .build();

        streamObserver.onNext(response);
        streamObserver.onCompleted();
    }

    @Override
    public void get(GetRequest request,
                    StreamObserver<GetResponse> streamObserver){
        String key = request.getKey();
        String value = memTable.get(key);

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
        AppendEntriesResponse response;

        if(request.getTerm() < raftNode.getTerm()){
            response = AppendEntriesResponse.newBuilder()
                    .setTerm(raftNode.getTerm())
                    .setSuccess(false)
                    .build();

            streamObserver.onNext(response);
            streamObserver.onCompleted();
            return;
        }
        raftNode.resetElectionTimer();

        for(String entry: request.getEntriesList()){
            raftNode.append(new LogEntry(raftNode.getTerm(), entry));
        }
        response = AppendEntriesResponse.newBuilder().setTerm(raftNode.getTerm())
                .setSuccess(true)
                .build();

        streamObserver.onNext(response);
        streamObserver.onCompleted();
    }
}
