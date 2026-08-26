// telemetry/metrics/MetricSnapshot.java
package telemetry.metrics;

public final class MetricSnapshot {
    private final long timestamp;
    private final long lastMTTRMillis;
    private final long electionCount;
    private final double avgCommitLatencyMillis;
    private final long throughputOpsPerSec;

    public MetricSnapshot(long timestamp, long lastMTTRMillis, long electionCount,
                          double avgCommitLatencyMillis, long throughputOpsPerSec) {
        this.timestamp = timestamp;
        this.lastMTTRMillis = lastMTTRMillis;
        this.electionCount = electionCount;
        this.avgCommitLatencyMillis = avgCommitLatencyMillis;
        this.throughputOpsPerSec = throughputOpsPerSec;
    }

    public long getTimestamp() { return timestamp; }
    public long getLastMTTRMillis() { return lastMTTRMillis; }
    public long getElectionCount() { return electionCount; }
    public double getAvgCommitLatencyMillis() { return avgCommitLatencyMillis; }
    public long getThroughputOpsPerSec() { return throughputOpsPerSec; }

    public String toCsvRow() {
        return timestamp + "," + lastMTTRMillis + "," + electionCount + ","
                + avgCommitLatencyMillis + "," + throughputOpsPerSec;
    }
}