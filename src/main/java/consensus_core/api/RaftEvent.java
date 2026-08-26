// consensus_core/api/RaftEvent.java
package consensus_core.api;

import java.util.Map;

public final class RaftEvent {
    private final EventType type;
    private final String nodeId;
    private final long term;
    private final long timestamp; // System.currentTimeMillis() at emission time
    private final Map<String, String> metadata; // e.g. {"leaderId": "node-3"}

    public RaftEvent(EventType type, String nodeId, long term, long timestamp, Map<String, String> metadata) {
        this.type = type;
        this.nodeId = nodeId;
        this.term = term;
        this.timestamp = timestamp;
        this.metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }

    public EventType getType() { return type; }
    public String getNodeId() { return nodeId; }
    public long getTerm() { return term; }
    public long getTimestamp() { return timestamp; }
    public Map<String, String> getMetadata() { return metadata; }
}