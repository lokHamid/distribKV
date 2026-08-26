// node_runner/NodeStatus.java
package node_runner;

import consensus_core.node.RaftNode;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A point-in-time, serializable snapshot of one node for the dashboard
 * ({@code GET /status}). Also the unit the metrics endpoint keys by.
 */
public final class NodeStatus {

    private final String nodeId;
    private final boolean running;
    private final String role;          // "LEADER" | "CANDIDATE" | "FOLLOWER" | "DOWN"
    private final long term;
    private final String leaderId;      // may be null
    private final long commitIndex;
    private final long lastApplied;
    private final long logLastIndex;
    private final String votedFor;      // may be null

    public NodeStatus(String nodeId, boolean running, String role, long term,
                      String leaderId, long commitIndex, long lastApplied, long logLastIndex, String votedFor) {
        this.nodeId = nodeId;
        this.running = running;
        this.role = role;
        this.term = term;
        this.leaderId = leaderId;
        this.commitIndex = commitIndex;
        this.lastApplied = lastApplied;
        this.logLastIndex = logLastIndex;
        this.votedFor = votedFor;
    }

    /** Builds a live status from a node, or a "DOWN" status when the node is stopped. */
    public static NodeStatus fromNode(String nodeId, RaftNode node) {
        if (node == null) {
            return new NodeStatus(nodeId, false, "DOWN", -1, null, -1, -1, -1, null);
        }
        return new NodeStatus(nodeId, true, node.getRole().name(), node.getCurrentTerm(),
                node.getLeaderId(), node.getCommitIndex(), node.getLastApplied(),
                node.getLogLastIndex(), node.getVotedFor());
    }

    public String getNodeId() { return nodeId; }
    public boolean isRunning() { return running; }
    public String getRole() { return role; }
    public long getTerm() { return term; }
    public String getLeaderId() { return leaderId; }
    public long getCommitIndex() { return commitIndex; }
    public long getLastApplied() { return lastApplied; }
    public long getLogLastIndex() { return logLastIndex; }
    public String getVotedFor() { return votedFor; }

    public Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("nodeId", nodeId);
        m.put("running", running);
        m.put("role", role);
        m.put("term", term);
        m.put("leaderId", leaderId);
        m.put("commitIndex", commitIndex);
        m.put("lastApplied", lastApplied);
        m.put("logLastIndex", logLastIndex);
        m.put("votedFor", votedFor);
        return m;
    }

    public String toJson() {
        return Json.write(toMap());
    }

    public static NodeStatus fromJson(String json) {
        Map<String, Object> m = Json.parseObject(json);
        return fromMap(m);
    }

    @SuppressWarnings("unchecked")
    public static NodeStatus fromMap(Map<String, Object> m) {
        return new NodeStatus(
                (String) m.get("nodeId"),
                (Boolean) m.getOrDefault("running", Boolean.FALSE),
                (String) m.getOrDefault("role", "DOWN"),
                num(m.get("term"), -1L),
                (String) m.get("leaderId"),
                num(m.get("commitIndex"), -1L),
                num(m.get("lastApplied"), -1L),
                num(m.get("logLastIndex"), -1L),
                (String) m.get("votedFor"));
    }

    private static long num(Object o, long def) {
        return o instanceof Number n ? n.longValue() : def;
    }
}
