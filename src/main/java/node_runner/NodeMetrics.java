// node_runner/NodeMetrics.java
package node_runner;

import telemetry.metrics.MetricSnapshot;

import java.util.LinkedHashMap;
import java.util.Map;

/** One node's {@link MetricSnapshot} tagged with its node id. */
public final class NodeMetrics {

    private final String nodeId;
    private final MetricSnapshot snapshot;

    public NodeMetrics(String nodeId, MetricSnapshot snapshot) {
        this.nodeId = nodeId;
        this.snapshot = snapshot;
    }

    public String getNodeId() { return nodeId; }
    public MetricSnapshot getSnapshot() { return snapshot; }

    public Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("nodeId", nodeId);
        m.put("timestamp", snapshot.getTimestamp());
        m.put("lastMTTRMillis", snapshot.getLastMTTRMillis());
        m.put("electionCount", snapshot.getElectionCount());
        m.put("avgCommitLatencyMillis", snapshot.getAvgCommitLatencyMillis());
        m.put("throughputOpsPerSec", snapshot.getThroughputOpsPerSec());
        return m;
    }

    public String toJson() {
        return Json.write(toMap());
    }
}
