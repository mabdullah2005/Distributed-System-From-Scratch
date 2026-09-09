package com.kvstore.network;

import com.kvstore.grpc.*;
import io.grpc.stub.StreamObserver;

import com.kvstore.storage.MemTable;

public class KVServiceImpl extends KVServiceGrpc.KVServiceImplBase{
    private MemTable memTable;

    public KVServiceImpl(MemTable memTable){
        this.memTable = memTable;
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> streamObserver){
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
    public void get(GetRequest request, StreamObserver<GetResponse> streamObserver){
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
}
