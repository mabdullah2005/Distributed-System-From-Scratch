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


        String key = request.getKey();
        String value = request.getValue();
        String command = key + ":" + value;
        LogEntry entry = new LogEntry(raftNode.getTerm(), command);
        raftNode.append(entry);

        raftNode.setCommitIndex(raftNode.getLastLogIndex());

        response = PutResponse.newBuilder()
                .setSuccessful(true)
                .build();

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
        AppendEntriesResponse response;
        int requestTerm = request.getTerm();
        int raftTerm = raftNode.getTerm();

        if(requestTerm < raftTerm){
            response = AppendEntriesResponse.newBuilder()
                    .setTerm(raftTerm)
                    .setSuccess(false)
                    .build();

            streamObserver.onNext(response);
            streamObserver.onCompleted();
            return;
        }
        raftNode.resetElectionTimer();
        raftNode.updateTerm(requestTerm);

        raftTerm = raftNode.getTerm();

        if(request.getPrevLogIndex() > raftNode.getLastLogIndex()
                || request.getPrevLogTerm() != raftNode.getLogAtIndex(request.getPrevLogIndex()).term()){
            response = AppendEntriesResponse.newBuilder()
                    .setTerm(raftTerm)
                    .setSuccess(false)
                    .build();

            streamObserver.onNext(response);
            streamObserver.onCompleted();
            return;
        }

        raftNode.truncateLogFromIndex(request.getPrevLogIndex());
        for(String entry: request.getEntriesList()){
            raftNode.append(new LogEntry(raftTerm, entry));
        }

        if(request.getLeaderCommitIndex() != raftNode.getCommitIndex()){
            int newIndex = Math.min(request.getLeaderCommitIndex(), raftNode.getLastLogIndex());
            raftNode.setCommitIndex(newIndex);
        }

        response = AppendEntriesResponse.newBuilder()
                .setTerm(raftTerm)
                .setSuccess(true)
                .build();

        streamObserver.onNext(response);
        streamObserver.onCompleted();
    }

    @Override
    public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> streamObserver){
        RequestVoteResponse response;

        if(request.getTerm() > raftNode.getTerm()){
            raftNode.resetElectionTimer();
            raftNode.updateTerm(request.getTerm());

            response = RequestVoteResponse.newBuilder()
                    .setTerm(raftNode.getTerm())
                    .setVoteGranted(true)
                    .build();

            streamObserver.onNext(response);
            streamObserver.onCompleted();

            return;
        }

        response = RequestVoteResponse.newBuilder()
                .setTerm(raftNode.getTerm())
                .setVoteGranted(false)
                .build();

        streamObserver.onNext(response);
        streamObserver.onCompleted();
    }
}
