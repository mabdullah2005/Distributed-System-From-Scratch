package com.kvstore.storage;

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

    public StorageEngine(MemTable memTable, String walPath) throws IOException {
        this.activeTable = memTable;
        immutableTables = new ArrayList<>();
        ssTables = new ArrayList<>();

        wal = new WriteAheadLog(walPath);
        lock = new ReentrantReadWriteLock();
    }

    public void put(String key, String value) throws IOException{
        lock.readLock().lock();

        try{
            wal.append("PUT", key, value);
            activeTable.put(key, value);
        }finally{
            lock.readLock().unlock();
        }

        checkAndTriggerFlush();
    }

    public String get(String key){
        lock.readLock().lock();

        try{
            String value = activeTable.get(key);
            if(value == null){
                for(int i = immutableTables.size() - 1; i>=0; i--){
                    MemTable table = immutableTables.get(i);
                    value = table.get(key);

                    if(value != null){
                        return value;
                    }
                }

                for(int i = ssTables.size() - 1; i>=0; i--){
                    SSTable table = ssTables.get(i);
                    value = table.get(key);

                    if(value != null){
                        return value;
                    }
                }
            }

            return value;
        }finally{
            lock.readLock().unlock();
        }
    }

    public void checkAndTriggerFlush(){
        if(activeTable.getTable().size() < 100){
            return;
        }

        lock.writeLock().lock();
        try{
            if(activeTable.getTable().size() < 100){
                return;
            }

            System.out.println("MemTable full! Swapping to immutable list...");

            immutableTables.add(activeTable);
            activeTable = new MemTable();
        }finally{
            lock.writeLock().unlock();
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
