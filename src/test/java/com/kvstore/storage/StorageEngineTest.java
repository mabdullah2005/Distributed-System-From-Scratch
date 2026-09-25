package com.kvstore.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    @Test
    void process_put_with_delimiters() throws IOException{
        engine.put("1|2", "Alice|\n PUT|1|Bob");
        StorageEngine engine2 = new StorageEngine(new MemTable(), pathFile);

        assertEquals("Alice|\n PUT|1|Bob", engine2.get("1|2"));
    }

    @Test
    void concurrent_calls() throws IOException, InterruptedException{
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch startGun = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(10);

        for(int i = 0; i<10; i++){
            final int id = i;

            executor.submit(() -> {
                try {
                    startGun.await();

                    engine.put("Key " + id, "Value " + id);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally{
                    doneLatch.countDown();
                }
            });
        }

        startGun.countDown();
        doneLatch.await();

        executor.shutdown();

        StorageEngine engine2 = new StorageEngine(new MemTable(), pathFile);

        for(int i = 0; i<10; i++){
            assertEquals("Value " + i, engine2.get("Key " + i));
        }
    }
}
