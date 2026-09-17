package com.kvstore.network;

import com.kvstore.grpc.AppendEntriesRequest;
import com.kvstore.grpc.AppendEntriesResponse;
import com.kvstore.grpc.RequestVoteRequest;
import com.kvstore.grpc.RequestVoteResponse;

public interface RaftRpcClient {
    AppendEntriesResponse sendAppendEntries(AppendEntriesRequest request);
    RequestVoteResponse sendRequestVote(RequestVoteRequest request);
}
