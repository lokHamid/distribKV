package consensus_core.node;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;

public final class RaftConfig {
    private final String nodeId;
    private final Set<String> peerIds;
    private final Path dataDir;

    private final int heartbeatIntervalMs;
    private final int minElectionTimeoutMs;
    private final int maxElectionTimeoutMs;
    private final int maxEntriesPerAppend;
    private final long snapshotThreshold;

    public RaftConfig(
            String nodeId,
            Set<String> peerIds,
            Path dataDir,
            int heartbeatIntervalMs,
            int minElectionTimeoutMs,
            int maxElectionTimeoutMs,
            int maxEntriesPerAppend,
            long snapshotThreshold) {

        // Safety check to satisfy Raft timing constraints: Broadcast Time << Election Timeout
        if (minElectionTimeoutMs <= heartbeatIntervalMs) {
            throw new IllegalArgumentException("minElectionTimeoutMs must be significantly larger than heartbeatIntervalMs");
        }

        this.nodeId = nodeId;
        this.peerIds = Collections.unmodifiableSet(peerIds);
        this.dataDir = dataDir;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.minElectionTimeoutMs = minElectionTimeoutMs;
        this.maxElectionTimeoutMs = maxElectionTimeoutMs;
        this.maxEntriesPerAppend = maxEntriesPerAppend;
        this.snapshotThreshold = snapshotThreshold;
    }

    // Standard immutable getters
    public String getNodeId() { return nodeId; }
    public Set<String> getPeerIds() { return peerIds; }
    public Path getDataDir() { return dataDir; }
    public int getHeartbeatIntervalMs() { return heartbeatIntervalMs; }
    public int getMinElectionTimeoutMs() { return minElectionTimeoutMs; }
    public int getMaxElectionTimeoutMs() { return maxElectionTimeoutMs; }
    public int getMaxEntriesPerAppend() { return maxEntriesPerAppend; }
    public long getSnapshotThreshold() { return snapshotThreshold; }
}
