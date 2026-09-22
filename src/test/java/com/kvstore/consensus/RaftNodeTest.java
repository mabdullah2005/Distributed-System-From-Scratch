package com.kvstore.consensus;

import com.kvstore.grpc.AppendEntriesRequest;
import com.kvstore.grpc.AppendEntriesResponse;
import com.kvstore.grpc.RequestVoteRequest;
import com.kvstore.grpc.RequestVoteResponse;
import com.kvstore.network.RaftRpcClient;
import com.kvstore.storage.MemTable;
import com.kvstore.storage.StorageEngine;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;

import com.kvstore.consensus.RaftNode.NodeState;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RaftNodeTest {
    private RaftNode node;
    private StorageEngine storageEngine;
    private final Integer myPort = 8081;
    private RaftRpcClient rpcClient;

    @TempDir
    private Path tempDir;
    private String walPath;

    @BeforeEach
    void setUp() throws IOException {
        walPath = tempDir.resolve("_wal.log").toString();

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

    @Test
    void quorum_succeeded(){
        RaftRpcClient client = new RaftRpcClient(){
            @Override
            public AppendEntriesResponse sendAppendEntries(AppendEntriesRequest request) {
                return AppendEntriesResponse.newBuilder()
                        .setSuccess(true)
                        .build();
            }

            @Override
            public RequestVoteResponse sendRequestVote(RequestVoteRequest request) {
                return null;
            }
        };

        RaftRpcClient client2 = new RaftRpcClient(){
            @Override
            public AppendEntriesResponse sendAppendEntries(AppendEntriesRequest request) {
                return AppendEntriesResponse.newBuilder()
                        .setSuccess(false)
                        .build();
            }

            @Override
            public RequestVoteResponse sendRequestVote(RequestVoteRequest request) {
                return null;
            }
        };

        RaftNode raftNode = new RaftNode(
                storageEngine,
                myPort,
                Arrays.asList(client, client2));

        raftNode.becomeLeader();
        boolean result = raftNode.replicateLog("1:abc");

        assertTrue(result);
        assertEquals(1, raftNode.getCommitIndex());
        assertEquals("abc", storageEngine.get("1"));
    }

    @Test
    void quorum_failed(){
        RaftRpcClient client = new RaftRpcClient(){
            @Override
            public AppendEntriesResponse sendAppendEntries(AppendEntriesRequest request) {
                return AppendEntriesResponse.newBuilder()
                        .setSuccess(false)
                        .build();
            }

            @Override
            public RequestVoteResponse sendRequestVote(RequestVoteRequest request) {
                return null;
            }
        };

        RaftRpcClient client2 = new RaftRpcClient(){
            @Override
            public AppendEntriesResponse sendAppendEntries(AppendEntriesRequest request) {
                return AppendEntriesResponse.newBuilder()
                        .setSuccess(false)
                        .build();
            }

            @Override
            public RequestVoteResponse sendRequestVote(RequestVoteRequest request) {
                return null;
            }
        };

        RaftNode raftNode = new RaftNode(
                storageEngine,
                myPort,
                Arrays.asList(client, client2));

        raftNode.becomeLeader();
        boolean result = raftNode.replicateLog("1:abc");

        assertFalse(result);
        assertEquals(0, raftNode.getCommitIndex());
        assertNull(storageEngine.get("1"));
    }

    @Test
    void non_leader_log_replication(){
        RaftRpcClient client = new RaftRpcClient(){
            @Override
            public AppendEntriesResponse sendAppendEntries(AppendEntriesRequest request) {
                return AppendEntriesResponse.newBuilder()
                        .setSuccess(false)
                        .build();
            }

            @Override
            public RequestVoteResponse sendRequestVote(RequestVoteRequest request) {
                return null;
            }
        };

        RaftRpcClient client2 = new RaftRpcClient(){
            @Override
            public AppendEntriesResponse sendAppendEntries(AppendEntriesRequest request) {
                return AppendEntriesResponse.newBuilder()
                        .setSuccess(false)
                        .build();
            }

            @Override
            public RequestVoteResponse sendRequestVote(RequestVoteRequest request) {
                return null;
            }
        };

        RaftNode raftNode = new RaftNode(
                storageEngine,
                myPort,
                Arrays.asList(client, client2));

        assertEquals(NodeState.FOLLOWER, raftNode.getState());
        boolean result = raftNode.replicateLog("1:abc");

        assertFalse(result);
        assertEquals(0, raftNode.getLastLogIndex());
        assertEquals(0, raftNode.getCommitIndex());
    }

    @Test
    void follower_replaces_divergent_entries() {
        node.append(new LogEntry(1, "k1:v1"));
        node.append(new LogEntry(1, "k2:v2_old"));

        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                .setTerm(2)
                .setLeaderId("leader")
                .setPrevLogIndex(1)
                .setPrevLogTerm(1)
                .addEntries("k2:v2_new")
                .setLeaderCommitIndex(2)
                .build();

        AppendEntriesResponse response = node.handleAppendEntry(request);

        assertTrue(response.getSuccess());
        assertEquals(2, node.getLastLogIndex());
        assertEquals("k2:v2_new", node.getLogAtIndex(2).command());
    }

    @Test
    void follower_rejects_when_prev_index_greater_than_last_index() {
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                .setTerm(1)
                .setLeaderId("leader")
                .setPrevLogIndex(2)
                .setPrevLogTerm(1)
                .addEntries("k3:v3")
                .setLeaderCommitIndex(0)
                .build();

        AppendEntriesResponse response = node.handleAppendEntry(request);

        assertFalse(response.getSuccess());
    }

    @Test
    void leader_synchronizes_log() {
        List<AppendEntriesRequest> laggingPeerRequests = new ArrayList<>();

        RaftRpcClient upToDatePeer = new RaftRpcClient() {
            @Override
            public AppendEntriesResponse sendAppendEntries(AppendEntriesRequest request) {
                return AppendEntriesResponse.newBuilder()
                        .setSuccess(true)
                        .setTerm(request.getTerm())
                        .build();
            }

            @Override
            public RequestVoteResponse sendRequestVote(RequestVoteRequest request) {
                return null;
            }
        };

        RaftRpcClient laggingPeer = new RaftRpcClient() {
            @Override
            public AppendEntriesResponse sendAppendEntries(AppendEntriesRequest request) {
                laggingPeerRequests.add(request);
                if (request.getPrevLogIndex() > 0) {
                    return AppendEntriesResponse.newBuilder()
                            .setSuccess(false)
                            .setTerm(request.getTerm())
                            .build();
                }
                return AppendEntriesResponse.newBuilder()
                        .setSuccess(true)
                        .setTerm(request.getTerm())
                        .build();
            }

            @Override
            public RequestVoteResponse sendRequestVote(RequestVoteRequest request) {
                return null;
            }
        };

        RaftNode leader = new RaftNode(
                storageEngine,
                myPort,
                Arrays.asList(upToDatePeer, laggingPeer));

        leader.becomeLeader();

        boolean firstReplicated = leader.replicateLog("k1:v1");
        assertTrue(firstReplicated);

        laggingPeerRequests.clear();

        boolean secondReplicated = leader.replicateLog("k2:v2");
        assertTrue(secondReplicated);

        assertTrue(laggingPeerRequests.size() >= 2, "Leader must retry with decremented nextIndex upon rejection");

        AppendEntriesRequest successfulRequest = laggingPeerRequests.get(laggingPeerRequests.size() - 1);
        assertEquals(0, successfulRequest.getPrevLogIndex());
        assertEquals(Arrays.asList("k1:v1", "k2:v2"), successfulRequest.getEntriesList());
    }
}
