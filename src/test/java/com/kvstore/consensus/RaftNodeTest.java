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

    @Test
    void vote_granted_if_havent_voted() throws IOException {
        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                .setCandidateId("abc")
                .setTerm(1)
                .setLastLogIndex(20)
                .setLastLogTerm(0)
                .build();

        RequestVoteResponse response = node.handleVoteRequest(request);

        assertEquals(true, response.getVoteGranted());
        assertEquals(1, response.getTerm());
        assertEquals(1, node.getTerm());
    }

    @Test
    void vote_rejected_if_already_voted(){
        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                .setCandidateId("abc")
                .setTerm(2)
                .setLastLogIndex(20)
                .setLastLogTerm(2)
                .build();

        RequestVoteRequest request2 = RequestVoteRequest.newBuilder()
                .setCandidateId("cba")
                .setTerm(2)
                .setLastLogIndex(20)
                .setLastLogTerm(2)
                .build();

        RequestVoteResponse response = node.handleVoteRequest(request);
        RequestVoteResponse response2 = node.handleVoteRequest(request2);
        RequestVoteResponse response3 = node.handleVoteRequest(request);

        assertTrue(response.getVoteGranted());
        assertFalse(response2.getVoteGranted());
        assertTrue(response3.getVoteGranted());
    }

    @Test
    void vote_rejected_if_log_stale(){
        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                .setCandidateId("abc")
                .setTerm(0)
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .build();

        node.append(new LogEntry(0, "def"));

        RequestVoteResponse response = node.handleVoteRequest(request);

        assertFalse(response.getVoteGranted());
    }

    @Test
    void multi_term_votes(){
        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                .setCandidateId("abc")
                .setTerm(1)
                .setLastLogIndex(2)
                .setLastLogTerm(0)
                .build();

        RequestVoteRequest request2 = RequestVoteRequest.newBuilder()
                .setCandidateId("def")
                .setTerm(2)
                .setLastLogIndex(5)
                .setLastLogTerm(1)
                .build();

        RequestVoteResponse response = node.handleVoteRequest(request);
        assertTrue(response.getVoteGranted());
        assertEquals(1, node.getTerm());

        RequestVoteResponse response2 = node.handleVoteRequest(request2);
        assertTrue(response2.getVoteGranted());
        assertEquals(2, node.getTerm());
    }
}
