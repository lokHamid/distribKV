// node_runner/NodeRunnerConfig.java
package node_runner;

import java.nio.file.Path;
import java.util.Map;

/**
 * Everything the node-runner needs to boot a single node: identity, peer
 * addresses (websocket URLs), storage location, ports and Raft timing.
 */
public final class NodeRunnerConfig {

    private final String nodeId;
    private final Map<String, String> peerAddresses; // peer id -> ws://host:port
    private final Path dataDir;
    private final int raftPort;
    private final int telemetryPort;
    private final int adminPort;
    private final String host;
    private final int heartbeatIntervalMs;
    private final int minElectionTimeoutMs;
    private final int maxElectionTimeoutMs;
    private final int maxEntriesPerAppend;
    private final long snapshotThreshold;

    public NodeRunnerConfig(String nodeId, Map<String, String> peerAddresses, Path dataDir,
                            int raftPort, int telemetryPort, int adminPort,
                            int heartbeatIntervalMs, int minElectionTimeoutMs, int maxElectionTimeoutMs,
                            int maxEntriesPerAppend, long snapshotThreshold) {
        this(nodeId, peerAddresses, dataDir, raftPort, telemetryPort, adminPort, "127.0.0.1",
                heartbeatIntervalMs, minElectionTimeoutMs, maxElectionTimeoutMs,
                maxEntriesPerAppend, snapshotThreshold);
    }

    public NodeRunnerConfig(String nodeId, Map<String, String> peerAddresses, Path dataDir,
                            int raftPort, int telemetryPort, int adminPort, String host,
                            int heartbeatIntervalMs, int minElectionTimeoutMs, int maxElectionTimeoutMs,
                            int maxEntriesPerAppend, long snapshotThreshold) {
        this.nodeId = nodeId;
        this.peerAddresses = Map.copyOf(peerAddresses);
        this.dataDir = dataDir;
        this.raftPort = raftPort;
        this.telemetryPort = telemetryPort;
        this.adminPort = adminPort;
        this.host = host;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.minElectionTimeoutMs = minElectionTimeoutMs;
        this.maxElectionTimeoutMs = maxElectionTimeoutMs;
        this.maxEntriesPerAppend = maxEntriesPerAppend;
        this.snapshotThreshold = snapshotThreshold;
    }

    public String getNodeId() { return nodeId; }
    public Map<String, String> getPeerAddresses() { return peerAddresses; }
    public Path getDataDir() { return dataDir; }
    public int getRaftPort() { return raftPort; }
    public int getTelemetryPort() { return telemetryPort; }
    public int getAdminPort() { return adminPort; }
    public String getHost() { return host; }
    public int getHeartbeatIntervalMs() { return heartbeatIntervalMs; }
    public int getMinElectionTimeoutMs() { return minElectionTimeoutMs; }
    public int getMaxElectionTimeoutMs() { return maxElectionTimeoutMs; }
    public int getMaxEntriesPerAppend() { return maxEntriesPerAppend; }
    public long getSnapshotThreshold() { return snapshotThreshold; }
}
