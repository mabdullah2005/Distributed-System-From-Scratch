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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class RaftNodeTest {
    private RaftNode node;
    private StorageEngine storageEngine;
    private final Integer myPort = 8081;
    private RaftRpcClient rpcClient;

    @TempDir
    private Path tempDir;
    private String walPath;

    private String newStatePath() {
        return tempDir.resolve("raft_state_" + java.util.UUID.randomUUID() + ".dat").toString();
    }

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

        node = new RaftNode(storageEngine, myPort, new ArrayList<>(Arrays.asList(rpcClient)), newStatePath());
    }

    @Test
    void consensus_check_node_initialisation(){
        assertEquals(0, node.getTerm());
        assertEquals(NodeState.FOLLOWER, node.getState());
    }

    @Test
    void consensus_start_election() {
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
    void vote_granted_if_havent_voted() {
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
    void quorum_succeeded() {
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
                Arrays.asList(client, client2),
                newStatePath());

        raftNode.becomeLeader();
        boolean result = raftNode.replicateLog("1:abc");

        assertTrue(result);
        assertEquals(1, raftNode.getCommitIndex());
        assertEquals("abc", storageEngine.get("1"));
    }

    @Test
    void quorum_failed() {
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
                Arrays.asList(client, client2),
                newStatePath());

        raftNode.becomeLeader();
        boolean result = raftNode.replicateLog("1:abc");

        assertFalse(result);
        assertEquals(0, raftNode.getCommitIndex());
        assertNull(storageEngine.get("1"));
    }

    @Test
    void non_leader_log_replication() {
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
                Arrays.asList(client, client2),
                newStatePath());

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
    void leader_synchronizes_log() throws InterruptedException {
        List<AppendEntriesRequest> laggingPeerRequests = new java.util.concurrent.CopyOnWriteArrayList<>();
        CountDownLatch firstSyncLatch = new CountDownLatch(1);
        CountDownLatch retryLatch = new CountDownLatch(2);
        java.util.concurrent.atomic.AtomicBoolean secondRound = new java.util.concurrent.atomic.AtomicBoolean(false);

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
                if (request.getEntriesList().isEmpty()) {
                    return AppendEntriesResponse.newBuilder()
                            .setSuccess(true)
                            .setTerm(request.getTerm())
                            .build();
                }
                laggingPeerRequests.add(request);
                if (secondRound.get()) {
                    retryLatch.countDown();
                } else {
                    firstSyncLatch.countDown();
                }
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
                Arrays.asList(upToDatePeer, laggingPeer),
                newStatePath());

        leader.becomeLeader();

        boolean firstReplicated = leader.replicateLog("k1:v1");
        assertTrue(firstReplicated);
        assertTrue(firstSyncLatch.await(1, TimeUnit.SECONDS), "Lagging peer should receive first replication entry");

        laggingPeerRequests.clear();
        secondRound.set(true);

        boolean secondReplicated = leader.replicateLog("k2:v2");
        assertTrue(secondReplicated);
        assertTrue(retryLatch.await(1, TimeUnit.SECONDS), "Leader must asynchronously retry with decremented nextIndex");

        assertTrue(laggingPeerRequests.size() >= 2, "Leader must retry with decremented nextIndex upon rejection");

        AppendEntriesRequest successfulRequest = laggingPeerRequests.get(laggingPeerRequests.size() - 1);
        assertEquals(0, successfulRequest.getPrevLogIndex());
        assertEquals(Arrays.asList("k1:v1", "k2:v2"), successfulRequest.getEntriesList());
    }

    @Test
    void linearizable_read_follower_rejects() {
        assertEquals(NodeState.FOLLOWER, node.getState());
        assertFalse(node.verifyLeadershipQuorum());
    }

    @Test
    void linearizable_read_leader_with_quorum_succeeds() {
        RaftRpcClient peer1 = new RaftRpcClient() {
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

        RaftRpcClient peer2 = new RaftRpcClient() {
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

        RaftNode leader = new RaftNode(storageEngine, myPort, Arrays.asList(peer1, peer2), newStatePath());
        leader.becomeLeader();

        boolean verified = leader.verifyLeadershipQuorum();
        assertTrue(verified);
    }

    @Test
    void linearizable_read_leader_partitioned_lacks_quorum_fails() {
        RaftRpcClient unreachablePeer1 = new RaftRpcClient() {
            @Override
            public AppendEntriesResponse sendAppendEntries(AppendEntriesRequest request) {
                return null;
            }

            @Override
            public RequestVoteResponse sendRequestVote(RequestVoteRequest request) {
                return null;
            }
        };

        RaftRpcClient unreachablePeer2 = new RaftRpcClient() {
            @Override
            public AppendEntriesResponse sendAppendEntries(AppendEntriesRequest request) {
                throw new RuntimeException("Connection timed out");
            }

            @Override
            public RequestVoteResponse sendRequestVote(RequestVoteRequest request) {
                return null;
            }
        };

        RaftNode partitionedLeader = new RaftNode(storageEngine, myPort, Arrays.asList(unreachablePeer1, unreachablePeer2), newStatePath());
        partitionedLeader.becomeLeader();

        boolean verified = partitionedLeader.verifyLeadershipQuorum();
        assertFalse(verified);
    }

    @Test
    void linearizable_read_leader_deposed_by_higher_term_steps_down() {
        RaftRpcClient higherTermPeer = new RaftRpcClient() {
            @Override
            public AppendEntriesResponse sendAppendEntries(AppendEntriesRequest request) {
                return AppendEntriesResponse.newBuilder()
                        .setSuccess(false)
                        .setTerm(request.getTerm() + 2)
                        .build();
            }

            @Override
            public RequestVoteResponse sendRequestVote(RequestVoteRequest request) {
                return null;
            }
        };

        RaftRpcClient normalPeer = new RaftRpcClient() {
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

        RaftNode leader = new RaftNode(storageEngine, myPort, Arrays.asList(higherTermPeer, normalPeer), newStatePath());
        leader.becomeLeader();
        int initialTerm = leader.getTerm();

        boolean verified = leader.verifyLeadershipQuorum();

        assertFalse(verified);
        assertEquals(NodeState.FOLLOWER, leader.getState());
        assertEquals(initialTerm + 2, leader.getTerm());
    }

    @Test
    void state_persistence_remembers_term_and_prevents_double_voting_after_crash() {
        String statePath = tempDir.resolve("raft_state_8081.dat").toString();

        RaftNode node1 = new RaftNode(storageEngine, myPort, Arrays.asList(rpcClient), statePath);

        RequestVoteRequest voteRequestFrom8082 = RequestVoteRequest.newBuilder()
                .setTerm(1)
                .setCandidateId("Port8082")
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .build();

        RequestVoteResponse resp1 = node1.handleVoteRequest(voteRequestFrom8082);
        assertTrue(resp1.getVoteGranted(), "Node should grant vote to first candidate in Term 1");

        RaftNode rebootedNode = new RaftNode(storageEngine, myPort, Arrays.asList(rpcClient), statePath);

        assertEquals(1, rebootedNode.getTerm(), "Rebooted node must recover its persisted term");

        RequestVoteRequest voteRequestFrom8083 = RequestVoteRequest.newBuilder()
                .setTerm(1)
                .setCandidateId("Port8083")
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .build();

        RequestVoteResponse resp2 = rebootedNode.handleVoteRequest(voteRequestFrom8083);
        assertFalse(resp2.getVoteGranted(), "Rebooted node MUST NOT vote for a different candidate in the same term!");

        RequestVoteResponse retryResp = rebootedNode.handleVoteRequest(voteRequestFrom8082);
        assertTrue(retryResp.getVoteGranted(), "Rebooted node can re-grant vote to the same candidate in the same term");
    }

    @Test
    void state_persistence_recovers_term_after_candidate_election() {
        String statePath = tempDir.resolve("raft_state_election.dat").toString();

        RaftNode node1 = new RaftNode(storageEngine, myPort, Arrays.asList(rpcClient), statePath);
        node1.startElection();
        int electionTerm = node1.getTerm();
        assertTrue(electionTerm >= 1);

        RaftNode rebootedNode = new RaftNode(storageEngine, myPort, Arrays.asList(rpcClient), statePath);
        assertEquals(electionTerm, rebootedNode.getTerm(), "Rebooted node must recover term incremented during election");

        RequestVoteRequest rivalVote = RequestVoteRequest.newBuilder()
                .setTerm(electionTerm)
                .setCandidateId("Port9999")
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .build();

        RequestVoteResponse resp = rebootedNode.handleVoteRequest(rivalVote);
        assertFalse(resp.getVoteGranted(), "Rebooted node voted for itself in this term, so must reject rival candidate");
    }

    @Test
    void follower_does_not_truncate_valid_entries_on_duplicate_or_delayed_rpc() {
        node.append(new LogEntry(1, "k1:v1"));
        node.append(new LogEntry(1, "k2:v2"));
        node.append(new LogEntry(1, "k3:v3"));

        AppendEntriesRequest delayedRequest = AppendEntriesRequest.newBuilder()
                .setTerm(1)
                .setLeaderId("leader")
                .setPrevLogIndex(1)
                .setPrevLogTerm(1)
                .addEntries("k2:v2")
                .setLeaderCommitIndex(1)
                .build();

        AppendEntriesResponse response = node.handleAppendEntry(delayedRequest);

        assertTrue(response.getSuccess());
        assertEquals(3, node.getLastLogIndex(), "Delayed duplicate RPC must NOT truncate later valid entries!");
        assertEquals("k1:v1", node.getLogAtIndex(1).command());
        assertEquals("k2:v2", node.getLogAtIndex(2).command());
        assertEquals("k3:v3", node.getLogAtIndex(3).command());
    }

    @Test
    void follower_does_not_truncate_logs_on_heartbeat_with_older_prev_log_index() {
        node.append(new LogEntry(1, "k1:v1"));
        node.append(new LogEntry(1, "k2:v2"));

        AppendEntriesRequest heartbeat = AppendEntriesRequest.newBuilder()
                .setTerm(1)
                .setLeaderId("leader")
                .setPrevLogIndex(0)
                .setPrevLogTerm(0)
                .setLeaderCommitIndex(0)
                .build();

        AppendEntriesResponse response = node.handleAppendEntry(heartbeat);

        assertTrue(response.getSuccess());
        assertEquals(2, node.getLastLogIndex(), "Empty heartbeat must NOT truncate existing unconflicted entries!");
    }

    @Test
    void replicateLog_broadcasts_to_all_peers_concurrently() {
        CountDownLatch allPeersArrived = new CountDownLatch(4);
        List<RaftRpcClient> peers = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            peers.add(new RaftRpcClient() {
                @Override
                public AppendEntriesResponse sendAppendEntries(AppendEntriesRequest request) {
                    allPeersArrived.countDown();
                    try {
                        boolean arrived = allPeersArrived.await(500, TimeUnit.MILLISECONDS);
                        if (!arrived) {
                            return AppendEntriesResponse.newBuilder()
                                    .setSuccess(false)
                                    .setTerm(request.getTerm())
                                    .build();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return null;
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
            });
        }

        RaftNode leader = new RaftNode(storageEngine, myPort, peers, newStatePath());
        leader.becomeLeader();

        boolean result = leader.replicateLog("key1:val1");
        assertTrue(result, "Concurrent replication should succeed because all 4 peers reach the rendezvous latch concurrently");
    }

    @Test
    void replicateLog_short_circuits_and_commits_on_majority_quorum_without_waiting_for_stragglers() {
        RaftRpcClient fastPeer1 = new RaftRpcClient() {
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

        RaftRpcClient fastPeer2 = new RaftRpcClient() {
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

        CountDownLatch slowPeerBlocker = new CountDownLatch(1);
        RaftRpcClient slowPeer1 = new RaftRpcClient() {
            @Override
            public AppendEntriesResponse sendAppendEntries(AppendEntriesRequest request) {
                try {
                    slowPeerBlocker.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return AppendEntriesResponse.newBuilder().setSuccess(true).setTerm(request.getTerm()).build();
            }

            @Override
            public RequestVoteResponse sendRequestVote(RequestVoteRequest request) {
                return null;
            }
        };

        RaftRpcClient slowPeer2 = new RaftRpcClient() {
            @Override
            public AppendEntriesResponse sendAppendEntries(AppendEntriesRequest request) {
                try {
                    slowPeerBlocker.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return AppendEntriesResponse.newBuilder().setSuccess(true).setTerm(request.getTerm()).build();
            }

            @Override
            public RequestVoteResponse sendRequestVote(RequestVoteRequest request) {
                return null;
            }
        };

        // Notice: slow peers are placed first and third in the list to expose sequential iteration
        RaftNode leader = new RaftNode(storageEngine, myPort, Arrays.asList(slowPeer1, fastPeer1, slowPeer2, fastPeer2), newStatePath());
        leader.becomeLeader();

        try {
            long startTime = System.currentTimeMillis();
            boolean success = leader.replicateLog("short_circuit:true");
            long elapsed = System.currentTimeMillis() - startTime;

            assertTrue(success, "Replication must succeed once majority quorum (leader + 2 peers) is reached");
            assertTrue(elapsed < 1000, "Replication must short-circuit and return immediately without waiting for slow peers! Took: " + elapsed + " ms");
            assertEquals("true", storageEngine.get("short_circuit"), "Committed value must be applied to state machine");
        } finally {
            slowPeerBlocker.countDown();
        }
    }
}
