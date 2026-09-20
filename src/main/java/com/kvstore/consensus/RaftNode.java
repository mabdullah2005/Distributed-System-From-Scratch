package com.kvstore.consensus;

import com.kvstore.grpc.*;
import com.kvstore.network.RaftRpcClient;
import com.kvstore.storage.StorageEngine;

import java.io.IOException;
import java.util.HashMap;
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
    private String votedFor;

    private final Integer myPort;
    private List<RaftRpcClient> peerPorts;
    private HashMap<RaftRpcClient, Integer> nextIndex;
    private HashMap<RaftRpcClient, Integer> matchIndex;

    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> currentTimer;

    private static final int MIN_TIMER = 150;
    private static final int MAX_TIMER = 300;

    public RaftNode(StorageEngine engine, Integer myPort, List<RaftRpcClient> peerPorts){
        this.term = 0;
        this.state = NodeState.FOLLOWER;
        this.commitIndex = 0;
        this.lastApplied = 0;

        this.myPort = myPort;
        this.peerPorts = peerPorts;
        this.engine = engine;

        this.raftLog = new ArrayList<>();
        this.votedFor = null;
        raftLog.add(new LogEntry(0, "dummy"));

        currentTimer = this.scheduler.schedule(this::startElection,
                ThreadLocalRandom.current().nextInt(MIN_TIMER, MAX_TIMER),
                TimeUnit.MILLISECONDS);
    }

    public Integer getPort(){
        return myPort;
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

    public synchronized void updateTerm(int newTerm){
        if(newTerm > term){
            term = newTerm;
            state = NodeState.FOLLOWER;
            votedFor = null;
        }
    }

    public void startElection(){
        synchronized(this){
            resetElectionTimer();
            term++;
            state = NodeState.CANDIDATE;
            votedFor = "Node" + myPort;
            System.out.println("Timer expired! Starting election for Term " + term);
        }

        broadcastRequestVote();
    }

    public synchronized void becomeLeader(){
        System.out.println("\n👑 I WON! I AM THE LEADER FOR TERM " + term + "!");
        state = NodeState.LEADER;

        nextIndex = new HashMap<>();
        matchIndex = new HashMap<>();

        for(RaftRpcClient peer: peerPorts){
            nextIndex.put(peer, getLastLogIndex()+1);
            matchIndex.put(peer, 0);
        }

        currentTimer.cancel(false);
        ScheduledFuture<?> heartbeatScheduler = scheduler.scheduleAtFixedRate(
                this::heartBeat,
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

    public synchronized void setCommitIndex(int newIndex){
        commitIndex = newIndex;
        applyCommittedLogs();
    }

    public synchronized void applyCommittedLogs(){
        while(commitIndex > lastApplied){
            lastApplied++;

            String command = getLogAtIndex(lastApplied).command();

            String[] split = command.split(":");
            String key = split[0];
            String value = split[1];

            try{
                engine.put(key, value);
            } catch(IOException e){
                e.printStackTrace();
            }
        }
    }

    public synchronized AppendEntriesResponse handleAppendEntry(AppendEntriesRequest request){
        int requestTerm = request.getTerm();
        int requestPrevIndex = request.getPrevLogIndex();
        int leaderCommitIndex = request.getLeaderCommitIndex();

        if(requestTerm >= getTerm()){
            updateTerm(requestTerm);
            state = NodeState.FOLLOWER;
            resetElectionTimer();

            if(requestPrevIndex <= getLastLogIndex() &&
                    request.getPrevLogTerm() == getLogAtIndex(requestPrevIndex).term()){

                truncateLogFromIndex(requestPrevIndex);

                for(String command: request.getEntriesList()){
                    LogEntry entry = new LogEntry(getTerm(), command);
                    append(entry);
                }

                if(getCommitIndex() < leaderCommitIndex){
                    setCommitIndex(Math.min(leaderCommitIndex, getLastLogIndex()));
                    applyCommittedLogs();
                }

                return AppendEntriesResponse.newBuilder()
                        .setTerm(getTerm())
                        .setSuccess(true)
                        .build();
            }
        }

        return AppendEntriesResponse.newBuilder()
                .setTerm(getTerm())
                .setSuccess(false)
                .build();
    }

    public synchronized RequestVoteResponse handleVoteRequest(RequestVoteRequest request){
        String candidateID = request.getCandidateId();
        int requestTerm = request.getTerm();
        int requestLastLogTerm = request.getLastLogTerm();
        int requestLastLogIndex = request.getLastLogIndex();
        int myTerm = getTerm();
        int myLastLogIndex = getLastLogIndex();
        int myLastLogTerm = getLogAtIndex(myLastLogIndex).term();

        if(requestTerm >= myTerm){
            updateTerm(requestTerm);
            if(votedFor == null || votedFor.equals(candidateID)){
                if(requestLastLogTerm > myLastLogTerm ||
                        (requestLastLogTerm == myLastLogTerm && requestLastLogIndex >= myLastLogIndex)){

                    resetElectionTimer();
                    votedFor = candidateID;

                    return RequestVoteResponse.newBuilder()
                            .setTerm(requestTerm)
                            .setVoteGranted(true)
                            .build();
                }
            }
        }

        return RequestVoteResponse.newBuilder()
                .setVoteGranted(false)
                .setTerm(getTerm())
                .build();
    }

    public synchronized boolean replicateLog(String command){
        if(getState() != NodeState.LEADER){
            return false;
        }
        LogEntry entry = new LogEntry(getTerm(), command);
        append(entry);
        int entryIndex = getLastLogIndex();
        int lastLogIndex = entryIndex - 1;
        int lastLogTerm = getLogAtIndex(lastLogIndex).term();

        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                .setTerm(getTerm())
                .setLeaderId("port" + getPort())
                .setPrevLogIndex(lastLogIndex)
                .setPrevLogTerm(lastLogTerm)
                .addEntries(command)
                .setLeaderCommitIndex(getCommitIndex())
                .build();

        int successCount = 1;
        for(RaftRpcClient peer: peerPorts){
            try{
                AppendEntriesResponse response = peer.sendAppendEntries(request);

                if(response == null){
                    continue;
                }

                while(!response.getSuccess()){
                    if(response.getTerm() > getTerm()){
                        state = NodeState.FOLLOWER;
                        updateTerm(response.getTerm());
                        resetElectionTimer();

                        return false;
                    }

                    int currentNext = nextIndex.get(peer);
                    if(currentNext <= 1){
                        break;
                    }

                    int newNext = currentNext - 1;
                    nextIndex.replace(peer, newNext);
                    int prevIndex = newNext - 1;
                    int prevTerm = getLogAtIndex(prevIndex).term();

                    List<String> entries = new ArrayList<>();
                    for(int i = newNext; i <= getLastLogIndex(); i++){
                        entries.add(getLogAtIndex(i).command());
                    }

                    AppendEntriesRequest retryRequest = AppendEntriesRequest.newBuilder()
                            .setTerm(getTerm())
                            .setLeaderId("port" + getPort())
                            .setPrevLogIndex(prevIndex)
                            .setPrevLogTerm(prevTerm)
                            .addAllEntries(entries)
                            .setLeaderCommitIndex(getCommitIndex())
                            .build();

                    response = peer.sendAppendEntries(retryRequest);
                    if(response == null){
                        break;
                    }
                }

                if(response != null && response.getSuccess()) {
                    nextIndex.replace(peer, getLastLogIndex() + 1);
                    matchIndex.replace(peer, getLastLogIndex());
                    successCount++;
                }
            } catch (Exception e) {
                System.out.println("Node is down");
            }
        }

        int majority = ((peerPorts.size() + 1)/2) + 1;
        if(successCount >= majority){
            setCommitIndex(entryIndex);
            applyCommittedLogs();

            return true;
        }

        return false;
    }

    public void broadcastRequestVote(){
        int voteCount = 1;

        for(RaftRpcClient client : peerPorts){
            RequestVoteRequest request = RequestVoteRequest.newBuilder()
                    .setTerm(term)
                    .setCandidateId("Port" + myPort)
                    .build();

            try{
                RequestVoteResponse response = client.sendRequestVote(request);

                if(response != null && response.getVoteGranted()){
                    voteCount++;
                }
            } catch(Exception e){
                System.out.println("Error: Port is not alive");
            }
        }

        int majority = ((peerPorts.size() + 1) / 2) + 1;

        if(voteCount >= majority){
            becomeLeader();
        }
    }

    public synchronized void heartBeat(){
        if(getState() != NodeState.LEADER){
            return;
        }

        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                .setTerm(getTerm())
                .setLeaderId("port" + getPort())
                .setPrevLogTerm(getLogAtIndex(getLastLogIndex()).term())
                .setPrevLogIndex(getLastLogIndex())
                .setLeaderCommitIndex(getCommitIndex())
                .build();

        for(RaftRpcClient peer: peerPorts){
            try{
                AppendEntriesResponse response = peer.sendAppendEntries(request);

                if(response != null && response.getSuccess()){
                    System.out.println("port accepted me as leader");
                }
            } catch (Exception e) {
                System.out.println("port is not alive");
            }
        }
    }
}
