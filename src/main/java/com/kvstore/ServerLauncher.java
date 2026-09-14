package com.kvstore;

import com.kvstore.consensus.RaftNode;
import com.kvstore.network.KVServiceImpl;
import com.kvstore.storage.MemTable;
import com.kvstore.storage.StorageEngine;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.ArrayList;

public class ServerLauncher {
    public static void main(String[] args) throws IOException, InterruptedException {
        Integer myPort = Integer.parseInt(args[0]);
        ArrayList<Integer> ports = new ArrayList<>();
        ArrayList<Integer> peerPorts = new ArrayList<>();

        for(String port: args){
            Integer portInt = Integer.parseInt(port);
            ports.add(portInt);

            if(portInt != myPort){
                peerPorts.add(portInt);
            }
        }

        MemTable memTable = new MemTable();
        StorageEngine storageEngine = new StorageEngine(memTable, "wal_" + myPort + ".log");
        RaftNode raftNode = new RaftNode(storageEngine, myPort, peerPorts);
        KVServiceImpl kvService = new KVServiceImpl(storageEngine, raftNode);

        Server server = ServerBuilder
                .forPort(myPort)
                .addService(kvService)
                .build()
                .start();

        System.out.println("Node " + myPort + " is online and listening...");
        server.awaitTermination();
    }
}
