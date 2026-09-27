package com.kvstore.storage;

import com.kvstore.grpc.WalRecord;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class WriteAheadLog {
    private FileChannel channel;
    private String filePath;
    private final OutputStream outputStream;

    private int unforcedWrites = 0;
    private static final int BATCH_SIZE = 100;

    public WriteAheadLog(String filePath) throws IOException {
        this.filePath = filePath;
        this.channel = FileChannel.open(
                Paths.get(filePath),
                StandardOpenOption.APPEND,
                StandardOpenOption.CREATE
        );

        outputStream = Channels.newOutputStream(channel);
    }

    public synchronized void append(String operation, String key, String value) throws IOException {
        WalRecord record = WalRecord.newBuilder()
                .setType(operation.equalsIgnoreCase("DEL") ? WalRecord.OpType.DEL : WalRecord.OpType.PUT)
                .setKey(key)
                .setValue(value)
                .build();

        record.writeDelimitedTo(outputStream);
        unforcedWrites++;

        if(unforcedWrites >= BATCH_SIZE){
            channel.force(true);
            unforcedWrites = 0;
        }
    }

    public synchronized void sync() throws IOException {
        if(unforcedWrites > 0){
            channel.force(true);
            unforcedWrites = 0;
        }
    }

    public List<WalRecord> readAll() throws IOException {
        List<WalRecord> records = new ArrayList<>();

        try(InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(filePath)))){
            while(true){
                WalRecord record = WalRecord.parseDelimitedFrom(in);
                if(record == null){
                    break;
                }

                records.add(record);
            }
        }

        return records;
    }
}
