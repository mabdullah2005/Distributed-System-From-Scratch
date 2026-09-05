package com.kvstore.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import static org.junit.jupiter.api.Assertions.*;

public class MemTableTest {
    private MemTable memTable;

    @BeforeEach
    void setUp(){
        memTable = new MemTable();
    }

    @Test
    void mem_table_put_and_get(){
        memTable.put("user1", "Alice");
        String response = memTable.get("user1");

        assertEquals("Alice", response);
    }

    @Test
    void mem_table_missing_key(){
        assertNull(memTable.get("user999"));
    }

    @Test
    void mem_table_delete(){
        memTable.put("user50", "Adam");
        assertEquals("Adam", memTable.get("user50"));

        memTable.delete("user50");
        assertNull(memTable.get("user50"));
    }
}
