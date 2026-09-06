package com.kvstore.storage;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import java.util.List;

public class WriteAheadLogTest {
    @TempDir
    Path TempDir;
    private WriteAheadLog wal;

    @BeforeEach
    void setUp(){
        String LogFilePath = TempDir.resolve("test_wal.log").toString();
        wal = new WriteAheadLog(LogFilePath);
    }

    @Test
    void Wal_put_and_delete(){
        wal.append("PUT", "user1", "Abigail");
        List<String> logState = wal.readAll();
        assertEquals("PUT|user1|Abigail", logState.get(0));

        wal.append("DEL", "user1", "Abigail");
        logState = wal.readAll();
        assertEquals("DEL|user1|Abigail", logState.get(logState.size() - 1));
    }
}
