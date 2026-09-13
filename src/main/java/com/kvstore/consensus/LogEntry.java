package com.kvstore.consensus;

public record LogEntry
        (int term,
         String command){
}
