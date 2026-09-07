package com.kvstore.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

public class WriteAheadLog {
    private FileChannel channel;
    private String filePath;

    public WriteAheadLog(String filePath) throws IOException {
        this.filePath = filePath;
        this.channel = FileChannel.open(
                Paths.get(filePath),
                StandardOpenOption.APPEND,
                StandardOpenOption.CREATE
        );
    }

    public void append(String operation, String key, String value) throws IOException {
        String newLine = operation + "|" + key + "|" + value + "\n";
        ByteBuffer buffer = ByteBuffer.wrap(newLine.getBytes());

        while(buffer.hasRemaining()){
            channel.write(buffer);
        }

        channel.force(true);
    }

    public List<String> readAll() throws IOException {
        return java.nio.file.Files.readAllLines(Paths.get(filePath));
    }
}
