package com.kvstore.consensus;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;

import com.kvstore.consensus.RaftNode.NodeState;

public class RaftNodeTest {
    private RaftNode node;

    @BeforeEach
    void setUp(){
        node = new RaftNode();
    }

    @Test
    void consensus_check_node_initialisation(){
        assertEquals(0, node.getTerm());
        assertEquals(NodeState.FOLLOWER, node.getState());
    }

    @Test
    void consensus_start_election(){
        node.startElection();

        assertEquals(1, node.getTerm());
        assertEquals(NodeState.CANDIDATE, node.getState());
    }

    @Test
    void consensus_become_leader(){
        node.becomeLeader();

        assertEquals(0, node.getTerm());
        assertEquals(NodeState.LEADER, node.getState());
    }
}
