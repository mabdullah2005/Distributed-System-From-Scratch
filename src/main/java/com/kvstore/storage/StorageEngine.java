package com.kvstore.storage;

import com.kvstore.grpc.WalRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.util.List;

public class StorageEngine implements StateMachine{
    private volatile MemTable activeTable;
    private List<MemTable> immutableTables;
    private List<SSTable> ssTables;
    private WriteAheadLog wal;
    private ReentrantReadWriteLock lock;

    public static final String TOMBSTONE = "__TOMBSTONE__";

    public StorageEngine(MemTable memTable, String walPath) throws IOException {
        this.activeTable = memTable;
        immutableTables = new ArrayList<>();
        ssTables = new ArrayList<>();

        wal = new WriteAheadLog(walPath);
        lock = new ReentrantReadWriteLock();

        recoverFromWal();
    }

    public void put(String key, String value) throws IOException{
        lock.readLock().lock();

        try{
            wal.append("PUT", key, value);
            activeTable.put(key, value);
        }finally{
            lock.readLock().unlock();
        }

        flush();
    }

    public void delete(String key) throws IOException{
        lock.readLock().lock();

        try{
            wal.append("DEL", key, TOMBSTONE);
            activeTable.put(key, TOMBSTONE);
        } finally {
            lock.readLock().unlock();
        }
    }

    public String get(String key){
        lock.readLock().lock();

        try{
            String value = activeTable.get(key);
            if(value == null) {
                for (int i = immutableTables.size() - 1; i >= 0; i--) {
                    MemTable table = immutableTables.get(i);
                    value = table.get(key);

                    if(value != null){
                        break;
                    }
                }
            }
            if(value == null){
                for(int i = ssTables.size() - 1; i>=0; i--){
                    SSTable table = ssTables.get(i);
                    value = table.get(key);

                    if(value != null){
                        break;
                    }
                }
            }

            if(value == null || value.equals(TOMBSTONE)){
                return null;
            }
            return value;
        }finally{
            lock.readLock().unlock();
        }
    }

    public boolean checkFlush(){
        return activeTable.getTable().size() < 100;
    }

    public void flush() throws IOException{
        if(checkFlush()){
            return;
        }

        lock.writeLock().lock();
        try{
            if(checkFlush()){
                return;
            }

            System.out.println("MemTable full! Swapping to immutable list...");

            immutableTables.add(activeTable);
            activeTable = new MemTable();
        } finally{
            lock.writeLock().unlock();
        }
    }

    public void forceFlush() throws IOException{
        lock.writeLock().lock();
        try{
            System.out.println("MemTable full! Swapping to immutable list...");

            immutableTables.add(activeTable);
            activeTable = new MemTable();
        } finally{
            lock.writeLock().unlock();
        }
    }

    public void recoverFromWal() throws IOException {
        List<WalRecord> persistantLogs = wal.readAll();

        for(WalRecord log: persistantLogs){
            if(log == null){
                continue;
            }

            if(log.getType().equals(WalRecord.OpType.DEL)){
                activeTable.put(log.getKey(), TOMBSTONE);
            } else if(log.getType().equals(WalRecord.OpType.PUT)){
                activeTable.put(log.getKey(), log.getValue());
            }
        }
    }

    @Override
    public void apply(String command) throws IOException{
        if(command == null){
            return;
        }

        String[] splitted = command.split(":", 2);
        if(splitted.length == 2){
            put(splitted[0], splitted[1]);
        }
    }
}
