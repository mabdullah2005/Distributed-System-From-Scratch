package com.kvstore.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentSkipListMap;

public class SSTable {
    private final ConcurrentSkipListMap<String, String> ssTable;
    private String filePath;

    public SSTable(MemTable memTable){
        this.ssTable = memTable.getTable();
    }

    public void flushToDisk(String filePath) throws IOException {
        this.filePath = filePath;

        try(FileChannel channel = FileChannel.open(
                Paths.get(filePath),
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        )){
            for (var entry: ssTable.entrySet()){
                String line = entry.getKey() + "|" + entry.getValue() + "\n";
                channel.write(ByteBuffer.wrap(line.getBytes()));
            }

            channel.force(true);
        }
    }
}
