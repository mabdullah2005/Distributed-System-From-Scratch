package com.kvstore.consensus;

import java.util.ArrayList;
import java.util.List;

public class RaftLog {
    List<LogEntry> logs;

    public RaftLog(){
        logs = new ArrayList<>();
        logs.add(new LogEntry(0, "dummy"));
    }

    public int getLastIndex(){ return logs.size() - 1; }

    public int getLastTerm(){ return logs.get(getLastIndex()).term(); }

    public synchronized LogEntry getEntry(int index){
        try {
            return logs.get(index);
        } catch (Exception e) {
            System.out.println("Index out of range for logs");
        }
        return null;
    }

    public synchronized void append(LogEntry entry){
        logs.add(entry);
    }

    public synchronized void appendAll(int term, List<String> entries){
        for(String entry: entries){
            append(new LogEntry(term, entry));
        }
    }

    public synchronized boolean hasMatchingEntry(int prevIndex, int term){
        if(prevIndex < 0 || prevIndex > getLastIndex()){
            return false;
        }

        return getEntry(prevIndex).term() == term;
    }

    public synchronized void truncateFromIndex(int index){
        try{
            logs.subList(index + 1, logs.size()).clear();
        } catch (Exception e) {
            System.out.println("Trucate failed.");
        }
    }

    public synchronized List<String> getCommandsFrom(int index){
        List<String> result = new ArrayList<>();
        try {
            for (int i = index; i < logs.size(); i++) {
                result.add(getEntry(i).command());
            }

            return result;
        } catch (Exception e) {
            System.out.println("getting entries list failed.");
        }

        return result;
    }

    public synchronized boolean isUpToDate(int lastLogIndex, int lastLogTerm){
        if(lastLogTerm != getLastTerm()){
            return lastLogTerm > getLastTerm();
        }

        return lastLogIndex >= getLastIndex();
    }

}
