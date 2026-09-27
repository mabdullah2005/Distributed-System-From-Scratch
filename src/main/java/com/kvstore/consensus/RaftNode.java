package com.kvstore.consensus;

import com.kvstore.grpc.*;
import com.kvstore.network.RaftRpcClient;
import com.kvstore.storage.StateMachine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;

public class RaftNode {
    public enum NodeState{
        FOLLOWER,
        CANDIDATE,
        LEADER
    };

    private String id;
    private int term;
    private NodeState state;
    private int commitIndex;
    private int lastApplied;
    private String votedFor;
    private StateMachine stateMachine;
    private RaftLog raftLog;
    private String stateFilePath;

    private final Integer myPort;
    private List<RaftRpcClient> peerPorts;
    private ConcurrentHashMap<RaftRpcClient, Integer> nextIndex;
    private ConcurrentHashMap<RaftRpcClient, Integer> matchIndex;

    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService rpcExecutor = Executors.newCachedThreadPool();
    private ScheduledFuture<?> currentTimer;
    private ScheduledFuture<?> heartbeatScheduler;

    private static final int MIN_TIMER = 150;
    private static final int MAX_TIMER = 300;

    public RaftNode(StateMachine stateMachine,
                    Integer myPort,
                    List<RaftRpcClient> peerPorts,
                    String stateFilePath){
        this.id = "Port" + myPort;
        this.state = NodeState.FOLLOWER;
        this.commitIndex = 0;
        this.lastApplied = 0;

        this.myPort = myPort;
        this.peerPorts = peerPorts;
        this.stateMachine = stateMachine;
        this.raftLog = new RaftLog();

        this.stateFilePath = stateFilePath;
        readStateFile();

        currentTimer = this.scheduler.schedule(this::startElection,
                ThreadLocalRandom.current().nextInt(MIN_TIMER, MAX_TIMER),
                TimeUnit.MILLISECONDS);
    }

    public RaftNode(StateMachine stateMachine, Integer myPort, List<RaftRpcClient> peerPorts){
        this(stateMachine, myPort, peerPorts, "raft_state_" + myPort + ".dat");
    }

    public void readStateFile(){
        try{
            if(stateFilePath != null && Files.exists(Paths.get(stateFilePath))){
                String fileState = Files.readString(Paths.get(stateFilePath));
                String[] splitted = fileState.split(":", 2);
                if(splitted.length >= 2){
                    this.term = Integer.parseInt(splitted[0]);
                    this.votedFor = splitted[1].isEmpty() ? null : splitted[1];
                    return;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Fatal: state file read failure", e);
        }

        this.term = 0;
        this.votedFor = null;
    }

    public void persistState(){
        try{
            String line = term + ":" + (votedFor != null ? votedFor : "");
            Files.writeString(Paths.get(stateFilePath), line);
        } catch (IOException e) {
            throw new RuntimeException("Fatal: Raft State failed to write on disk", e);
        }
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

        persistState();
    }

    public synchronized int getLastLogIndex(){
        return raftLog.getLastIndex();
    }

    public synchronized LogEntry getLogAtIndex(int index){
        return raftLog.getEntry(index);
    }

    public synchronized void append(LogEntry entry){
        raftLog.append(entry);
    }

    public synchronized void setCommitIndex(int newIndex){
        commitIndex = newIndex;
        applyCommittedLogs();
    }

    public synchronized void applyCommittedLogs(){
        while(commitIndex > lastApplied){
            lastApplied++;

            String command = getLogAtIndex(lastApplied).command();

            try{
                stateMachine.apply(command);
            } catch(IOException e){
                e.printStackTrace();
            }
        }
    }

    private AppendEntriesRequest buildAppendRequest(int index){
        return AppendEntriesRequest.newBuilder()
                .setTerm(getTerm())
                .setLeaderId(id)
                .setPrevLogIndex(index - 1)
                .setPrevLogTerm(raftLog.getEntry(index - 1).term())
                .addAllEntries(raftLog.getCommandsFrom(index))
                .setLeaderCommitIndex(getCommitIndex())
                .build();
    }

    private synchronized boolean hasQuorum(int count){
        int majority = ((peerPorts.size() + 1)/2) + 1;
        return count >= majority;
    }

    /*
    * Election Methods
    * */

    public void stepDown(int newTerm){
        updateTerm(newTerm);
        state = NodeState.FOLLOWER;
        resetElectionTimer();

        if(heartbeatScheduler != null){
            heartbeatScheduler.cancel(false);
        }
    }

    public synchronized void resetElectionTimer(){
        currentTimer.cancel(false);

        currentTimer = this.scheduler.schedule(this::startElection,
                ThreadLocalRandom.current().nextInt(MIN_TIMER, MAX_TIMER),
                TimeUnit.MILLISECONDS);
    }

    public void startElection(){
        synchronized(this){
            resetElectionTimer();
            term++;
            state = NodeState.CANDIDATE;
            votedFor = id;
            System.out.println("Timer expired! Starting election for Term " + term);
            persistState();
        }

        broadcastRequestVote();
    }

    public synchronized void becomeLeader(){
        System.out.println("\n👑 I WON! I AM THE LEADER FOR TERM " + term + "!");
        state = NodeState.LEADER;

        nextIndex = new ConcurrentHashMap<>();
        matchIndex = new ConcurrentHashMap<>();

        for(RaftRpcClient peer: peerPorts){
            nextIndex.put(peer, getLastLogIndex()+1);
            matchIndex.put(peer, 0);
        }

        currentTimer.cancel(false);
        heartbeatScheduler = scheduler.scheduleAtFixedRate(
                this::heartBeat,
                0,
                50,
                TimeUnit.MILLISECONDS);
    }

    /*
        Methods that followers are called by leaders
     */

    public synchronized AppendEntriesResponse handleAppendEntry(AppendEntriesRequest request){
        int requestTerm = request.getTerm();
        int requestPrevIndex = request.getPrevLogIndex();
        int leaderCommitIndex = request.getLeaderCommitIndex();

        if(requestTerm >= getTerm()){
            stepDown(requestTerm);

            if(raftLog.hasMatchingEntry(requestPrevIndex, request.getPrevLogTerm())){

                raftLog.replicateEntries(requestPrevIndex, requestTerm, request.getEntriesList());

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
        int myTerm = getTerm();

        if(requestTerm >= myTerm){
            updateTerm(requestTerm);
            if(votedFor == null || votedFor.equals(candidateID)){
                if(raftLog.isUpToDate(request.getLastLogIndex(), request.getLastLogTerm())){

                    resetElectionTimer();
                    votedFor = candidateID;
                    persistState();

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

    /*
        Methods leaders call
     */

    public boolean verifyLeadershipQuorum(){
        if(getState() != NodeState.LEADER){
            return false;
        }

        AppendEntriesRequest request = buildAppendRequest(getLastLogIndex() + 1);

        int majority = ((peerPorts.size() + 1) / 2) + 1;
        CountDownLatch quorumLatch = new CountDownLatch(majority - 1);
        AtomicInteger successCount = new AtomicInteger(1);

        for (RaftRpcClient peer : peerPorts) {
            rpcExecutor.submit(() -> {
                try {
                    AppendEntriesResponse response = peer.sendAppendEntries(request);

                    if (response != null) {
                        if (response.getTerm() > getTerm()) {
                            stepDown(response.getTerm());
                            return;
                        }
                        if (response.getSuccess()) {
                            successCount.incrementAndGet();
                            quorumLatch.countDown();
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Node is not alive");
                }
            });
        }

        try {
            quorumLatch.await(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }

        if (getState() != NodeState.LEADER) {
            return false;
        }

        return hasQuorum(successCount.get());
    }

    public GetResponse get(String key){
        if(!verifyLeadershipQuorum()){
            return GetResponse.newBuilder()
                    .setSuccessful(false)
                    .build();
        }

        String value = stateMachine.get(key);

        if(value == null || value.isBlank()){
            return GetResponse.newBuilder()
                    .setSuccessful(true)
                    .setFound(false)
                    .build();
        }

        return GetResponse.newBuilder()
                .setSuccessful(true)
                .setFound(true)
                .setValue(value)
                .build();
    }


    public boolean replicateLog(String command){
        int entryIndex;

        if(getState() != NodeState.LEADER){
            return false;
        }

        synchronized(this){
            LogEntry entry = new LogEntry(getTerm(), command);
            append(entry);
            entryIndex = getLastLogIndex();
        }

        AppendEntriesRequest request = buildAppendRequest(entryIndex);

        int majority = ((peerPorts.size() + 1) / 2) + 1;
        CountDownLatch quorumLatch = new CountDownLatch(majority - 1);
        AtomicInteger successCount = new AtomicInteger(1);

        for (RaftRpcClient peer : peerPorts) {
            rpcExecutor.submit(() -> {
                try {
                    AppendEntriesResponse response = peer.sendAppendEntries(request);

                    if (response == null) {
                        return;
                    }

                    while (!response.getSuccess()) {
                        if (response.getTerm() > getTerm()) {
                            stepDown(response.getTerm());
                            return;
                        }

                        Integer currentNext = nextIndex != null ? nextIndex.get(peer) : null;
                        if (currentNext == null) {
                            currentNext = entryIndex;
                        }
                        if (currentNext <= 1) {
                            break;
                        }

                        int newNext = currentNext - 1;
                        nextIndex.replace(peer, newNext);
                        AppendEntriesRequest retryRequest = buildAppendRequest(newNext);

                        response = peer.sendAppendEntries(retryRequest);
                        if (response == null) {
                            break;
                        }
                    }

                    if (response != null && response.getSuccess()) {
                        nextIndex.replace(peer, getLastLogIndex() + 1);
                        matchIndex.replace(peer, getLastLogIndex());
                        successCount.incrementAndGet();
                        quorumLatch.countDown();
                    }
                } catch (Exception e) {
                    System.out.println("Node is down");
                }
            });
        }

        try {
            quorumLatch.await(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }

        if (hasQuorum(successCount.get())) {
            setCommitIndex(entryIndex);
            return true;
        }

        return false;
    }

    public void broadcastRequestVote(){
        int voteCount = 1;

        for(RaftRpcClient client : peerPorts){
            RequestVoteRequest request = RequestVoteRequest.newBuilder()
                    .setTerm(term)
                    .setCandidateId(id)
                    .setLastLogTerm(raftLog.getLastTerm())
                    .setLastLogIndex(raftLog.getLastIndex())
                    .build();

            try{
                RequestVoteResponse response = client.sendRequestVote(request);

                if(response != null){
                    if(response.getTerm() > getTerm()){
                        stepDown(response.getTerm());
                        return;
                    }

                    if(response.getVoteGranted()){
                        voteCount++;
                    }
                }
            } catch(Exception e){
                System.out.println("Error: Port is not alive");
            }
        }

        if(hasQuorum(voteCount) && getState() == NodeState.CANDIDATE){
            becomeLeader();
        }
    }

    public void heartBeat(){
        if(getState() != NodeState.LEADER){
            return;
        }

        AppendEntriesRequest request = buildAppendRequest(getLastLogIndex() + 1);

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
