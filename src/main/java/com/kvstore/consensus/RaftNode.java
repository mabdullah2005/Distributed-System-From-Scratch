package com.kvstore.consensus;

import com.kvstore.grpc.*;
import com.kvstore.network.RaftRpcClient;
import com.kvstore.storage.StateMachine;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.*;
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

    private final Integer myPort;
    private List<RaftRpcClient> peerPorts;
    private HashMap<RaftRpcClient, Integer> nextIndex;
    private HashMap<RaftRpcClient, Integer> matchIndex;

    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> currentTimer;
    private ScheduledFuture<?> heartbeatScheduler;

    private static final int MIN_TIMER = 150;
    private static final int MAX_TIMER = 300;

    public RaftNode(StateMachine stateMachine, Integer myPort, List<RaftRpcClient> peerPorts){
        this.id = "Port" + myPort;
        this.term = 0;
        this.state = NodeState.FOLLOWER;
        this.commitIndex = 0;
        this.lastApplied = 0;

        this.myPort = myPort;
        this.peerPorts = peerPorts;
        this.stateMachine = stateMachine;

        this.raftLog = new RaftLog();
        this.votedFor = null;

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

    public synchronized void updateTerm(int newTerm){
        if(newTerm > term){
            term = newTerm;
            state = NodeState.FOLLOWER;
            votedFor = null;
        }
    }

    public synchronized int getLastLogIndex(){
        return raftLog.getLastIndex();
    }

    public synchronized LogEntry getLogAtIndex(int index){
        return raftLog.getEntry(index);
    }

    public synchronized void truncateLogFromIndex(int index){
        raftLog.truncateFromIndex(index);
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
                truncateLogFromIndex(requestPrevIndex);

                raftLog.appendAll(getTerm(), request.getEntriesList());

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

    public synchronized boolean replicateLog(String command){
        if(getState() != NodeState.LEADER){
            return false;
        }
        LogEntry entry = new LogEntry(getTerm(), command);
        append(entry);
        int entryIndex = getLastLogIndex();

        AppendEntriesRequest request = buildAppendRequest(entryIndex);

        int successCount = 1;
        for(RaftRpcClient peer: peerPorts){
            try{
                AppendEntriesResponse response = peer.sendAppendEntries(request);

                if(response == null){
                    continue;
                }

                while(!response.getSuccess()){
                    if(response.getTerm() > getTerm()){
                        stepDown(response.getTerm());
                        return false;
                    }

                    int currentNext = nextIndex.get(peer);
                    if(currentNext <= 1){
                        break;
                    }

                    int newNext = currentNext - 1;
                    nextIndex.replace(peer, newNext);
                    AppendEntriesRequest retryRequest = buildAppendRequest(newNext);

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

        if(hasQuorum(successCount)){
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

    public synchronized void heartBeat(){
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
