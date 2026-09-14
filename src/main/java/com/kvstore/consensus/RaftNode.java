package com.kvstore.consensus;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.kvstore.grpc.AppendEntriesRequest;
import com.kvstore.grpc.AppendEntriesResponse;
import com.kvstore.grpc.KVServiceGrpc;
import com.kvstore.storage.MemTable;
import com.kvstore.storage.StorageEngine;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.List;
import java.util.ArrayList;

public class RaftNode {
    public enum NodeState{
        FOLLOWER,
        CANDIDATE,
        LEADER
    };
    private int term;
    private NodeState state;
    private int commitIndex;
    private int lastApplied;
    private StorageEngine engine;
    private List<LogEntry> raftLog;

    private final Integer myPort;
    private List<Integer> peerPorts;

    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> currentTimer;

    private static final int MIN_TIMER = 150;
    private static final int MAX_TIMER = 300;

    public RaftNode(StorageEngine engine, Integer myPort, List<Integer> peerPorts){
        this.term = 0;
        this.state = NodeState.FOLLOWER;
        this.commitIndex = 0;
        this.lastApplied = 0;

        this.myPort = myPort;
        this.peerPorts = peerPorts;
        this.engine = engine;

        this.raftLog = new ArrayList<>();
        raftLog.add(new LogEntry(0, "dummy"));

        currentTimer = this.scheduler.schedule(this::startElection,
                ThreadLocalRandom.current().nextInt(MIN_TIMER, MAX_TIMER),
                TimeUnit.MILLISECONDS);
    }

    public synchronized int getTerm(){
        return term;
    }

    public synchronized NodeState getState(){
        return state;
    }

    public synchronized int getCommitIndex(){
        return commitIndex;
    }

    public synchronized void setCommitIndex(int newIndex){
        commitIndex = newIndex;
        applyCommittedLogs();
    }

    public synchronized void startElection(){
        term++;
        state = NodeState.CANDIDATE;
    }

    public synchronized void becomeLeader(){
        state = NodeState.LEADER;

        currentTimer.cancel(false);
        ScheduledFuture<?> heartbeatScheduler = scheduler.scheduleAtFixedRate(
                this::broadcastAppendEntries,
                0,
                50,
                TimeUnit.MILLISECONDS);
    }

    public synchronized void resetElectionTimer(){
        currentTimer.cancel(false);

        currentTimer = this.scheduler.schedule(this::startElection,
                ThreadLocalRandom.current().nextInt(MIN_TIMER, MAX_TIMER),
                TimeUnit.MILLISECONDS);
    }

    public synchronized int getLastLogIndex(){
        return raftLog.size() - 1;
    }

    public synchronized LogEntry getLogAtIndex(int index){
        return raftLog.get(index);
    }

    public synchronized void truncateLogFromIndex(int index){
        raftLog.subList(index + 1, raftLog.size()).clear();
    }

    public synchronized void append(LogEntry entry){
        raftLog.add(entry);
    }

    public synchronized void applyCommittedLogs(){
        while(commitIndex > lastApplied){
            lastApplied++;

            String command = getLogAtIndex(lastApplied).command();

            String [] split = command.split(":");
            String key = split[0];
            String value = split[1];

            try{
                engine.put(key, value);
            }catch(IOException e){
                e.printStackTrace();
            }
        }
    }

    public void broadcastAppendEntries(){
        if(state != NodeState.LEADER){
            return;
        }

        for(Integer port: peerPorts){
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress("localhost", port)
                    .usePlaintext()
                    .build();

            KVServiceGrpc.KVServiceBlockingStub stub = KVServiceGrpc.newBlockingStub(channel);

            AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                    .setTerm(term)
                    .setLeaderId("port " + myPort)
                    .setPrevLogIndex(getLastLogIndex())
                    .setPrevLogTerm(getLogAtIndex(getLastLogIndex()).term())
                    .setLeaderCommitIndex(commitIndex)
                    .build();

            try{
                AppendEntriesResponse response = stub.appendEntries(request);
                if(response.getSuccess()){
                    System.out.println("Node " + port + "accepted the logs!");
                }
            } catch (Exception e) {
                System.out.println("Error: port" + port + "did not accept the logs");
            } finally{
                channel.shutdown();
            }
        }
    }
}
