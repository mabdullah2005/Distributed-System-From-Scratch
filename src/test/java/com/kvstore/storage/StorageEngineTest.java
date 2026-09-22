package com.kvstore.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class StorageEngineTest {
    @TempDir
    private Path temp;
    private String pathFile;

    private StorageEngine engine;

    @BeforeEach
    void setUp() throws IOException {
        pathFile = temp.resolve("_wal.log").toString();
        engine = new StorageEngine(new MemTable(), pathFile);
    }

    @Test
    void replays_wal_on_startup() throws IOException{
        engine.put("1", "happy");
        engine.put("2", "sad");
        engine.put("3", "excited");

        MemTable memTable = new MemTable();

        StorageEngine engine2 = new StorageEngine(memTable, pathFile);

        assertEquals("happy", engine2.get("1"));
        assertEquals("sad", engine2.get("2"));
        assertEquals("excited", engine2.get("3"));
    }

    @Test
    void tombstone_returns_null() throws IOException{
        engine.put("1", "Alice");
        engine.forceFlush();

        engine.delete("1");

        assertNull(engine.get("1"));
    }

    @Test
    void tombstone_after_crash_recovery() throws IOException{
        engine.put("1", "Alice");
        engine.put("2", "Oscar");
        engine.delete("1");

        StorageEngine engine2 = new StorageEngine(new MemTable(), pathFile);

        assertNull(engine2.get("1"));
        assertEquals("Oscar", engine2.get("2"));
    }
}
