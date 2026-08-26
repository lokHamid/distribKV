package node_runner;

import consensus_core.node.RaftNode;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class NodeStatusTest {

    @Test
    void downStatusWhenNodeStopped() {
        NodeStatus s = NodeStatus.fromNode("node-1", null);
        assertFalse(s.isRunning());
        assertEquals("DOWN", s.getRole());
        assertEquals(-1, s.getTerm());
    }

    @Test
    void jsonRoundTrip() {
        NodeStatus s = new NodeStatus("node-2", true, "LEADER", 4, "node-2", 7, 7, 9, "node-2");
        String json = s.toJson();
        NodeStatus parsed = NodeStatus.fromJson(json);
        assertEquals(s.getNodeId(), parsed.getNodeId());
        assertEquals(s.getRole(), parsed.getRole());
        assertEquals(s.getTerm(), parsed.getTerm());
        assertEquals(s.getCommitIndex(), parsed.getCommitIndex());
        assertTrue(parsed.isRunning());
    }
}
