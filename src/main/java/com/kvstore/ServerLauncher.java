package com.kvstore;

import com.kvstore.consensus.RaftNode;
import com.kvstore.network.GrpcRaftClient;
import com.kvstore.network.KVServiceImpl;
import com.kvstore.network.RaftRpcClient;
import com.kvstore.storage.MemTable;
import com.kvstore.storage.StorageEngine;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.ArrayList;

public class ServerLauncher {
    public static void main(String[] args) throws IOException, InterruptedException {
        Integer myPort = Integer.parseInt(args[0]);
        ArrayList<RaftRpcClient> peerPorts = new ArrayList<>();

        for(int i = 1; i<args.length; i++){
            Integer portInt = Integer.parseInt(args[i]);

            peerPorts.add(new GrpcRaftClient(portInt));
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
