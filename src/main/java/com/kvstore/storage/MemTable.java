package com.kvstore.storage;

import java.util.concurrent.ConcurrentSkipListMap;

public class MemTable {
    private ConcurrentSkipListMap<String, String> memTable;

    public MemTable(){
        memTable = new ConcurrentSkipListMap<>();
    }

    public void put(String key, String value){
        memTable.put(key, value);
    }

    public String get(String key){
        return memTable.get(key);
    }

    public void delete(String key){
        memTable.remove(key);
    }

    public ConcurrentSkipListMap<String, String> getTable(){
        return memTable;
    }
}
