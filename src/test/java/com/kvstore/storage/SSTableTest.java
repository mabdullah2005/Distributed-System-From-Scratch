package com.kvstore.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.List;

public class SSTableTest{
    private MemTable memTable;
    private SSTable ssTable;

    @TempDir
    private Path tempDir;
    private String pathFile;

    @BeforeEach
    void setUp(){
        pathFile = tempDir.resolve("test_ssTable.log").toString();
        memTable = new MemTable();
    }

    @Test
    void ssTable_flush() throws IOException {
        memTable.put("Rhino", "grey");
        memTable.put("Zebra", "black and white");
        memTable.put("Ant", "brown");

        SSTable ssTable = new SSTable(memTable);
        ssTable.flushToDisk(pathFile);

        List<String> lines = java.nio.file.Files.readAllLines(Paths.get(pathFile));

        assertEquals("Ant|brown", lines.get(0));
        assertEquals("Rhino|grey", lines.get(1));
        assertEquals("Zebra|black and white", lines.get(2));
    }
}
