package com.kvstore.consensus;

import com.kvstore.grpc.AppendEntriesRequest;
import com.kvstore.grpc.AppendEntriesResponse;
import com.kvstore.grpc.RequestVoteRequest;
import com.kvstore.grpc.RequestVoteResponse;
import com.kvstore.network.GrpcRaftClient;
import com.kvstore.network.RaftRpcClient;
import com.kvstore.storage.MemTable;
import com.kvstore.storage.StorageEngine;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;

import com.kvstore.consensus.RaftNode.NodeState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class RaftNodeTest {
    private RaftNode node;
    private StorageEngine storageEngine;
    private final Integer myPort = 8081;
    private RaftRpcClient rpcClient;
    private String walPath = "_wal.log";

    @BeforeEach
    void setUp() throws IOException {
        storageEngine = new StorageEngine(new MemTable(), walPath);
        rpcClient = new RaftRpcClient() {
            @Override
            public AppendEntriesResponse sendAppendEntries(AppendEntriesRequest request) {
                return null;
            }

            @Override
            public RequestVoteResponse sendRequestVote(RequestVoteRequest request) {
                return RequestVoteResponse
                        .newBuilder()
                        .setVoteGranted(false)
                        .build();
            }
        };

        node = new RaftNode(storageEngine, myPort, new ArrayList<>(Arrays.asList(rpcClient)));
    }

    @Test
    void consensus_check_node_initialisation(){
        assertEquals(0, node.getTerm());
        assertEquals(NodeState.FOLLOWER, node.getState());
    }

    @Test
    void consensus_start_election(){
        node.startElection();

        assertEquals(1, node.getTerm());
        assertEquals(NodeState.CANDIDATE, node.getState());
    }

    @Test
    void consensus_become_leader(){
        node.becomeLeader();

        assertEquals(0, node.getTerm());
        assertEquals(NodeState.LEADER, node.getState());
    }
}
